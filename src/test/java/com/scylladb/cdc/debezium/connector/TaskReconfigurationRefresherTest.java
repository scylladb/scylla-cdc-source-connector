package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.config.Configuration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

class TaskReconfigurationRefresherTest {

  private static final long INTERVAL_MS = 120_000;

  @Test
  void addsNoRetryChainWhileTaskConfigsKeepsFailing() {
    FakeConnect connect = new FakeConnect(Failure.IN_TASK_CONFIGS);
    connect.run(TimeUnit.HOURS.toMillis(1));
    assertEquals(0, connect.refresherRequests);
    assertEquals(1, connect.chains.size());
  }

  @Test
  void addsABoundedNumberOfRetryChainsWhenFailingRetriesAreDelayed() {
    FakeConnect connect = new FakeConnect(Failure.IN_TASK_CONFIGS);
    connect.queueDelayMs = 3 * INTERVAL_MS; // Retries arrive less often than the interval.
    connect.run(TimeUnit.HOURS.toMillis(1));
    int max = TaskReconfigurationRefresher.MAX_REQUESTS_WITHOUT_SUCCESS;
    assertEquals(max, connect.refresherRequests);
    assertEquals(max + 1, connect.chains.size());
  }

  @Test
  void recoversWhenAFailingChainEndsWithoutSucceeding() {
    FakeConnect connect = new FakeConnect(Failure.IN_TASK_CONFIGS);
    connect.failuresLeft = 1;
    connect.skipsAfterFailure = 1; // The retry is skipped, so the chain ends silently.
    connect.run(TimeUnit.HOURS.toMillis(1));
    assertTrue(connect.published);
    assertTrue(connect.chains.isEmpty());
  }

  @Test
  void addsABoundedNumberOfRetryChainsWhenFailingBeforeTaskConfigs() {
    FakeConnect connect = new FakeConnect(Failure.BEFORE_TASK_CONFIGS);
    connect.run(TimeUnit.HOURS.toMillis(1));
    int max = TaskReconfigurationRefresher.MAX_REQUESTS_WITHOUT_SUCCESS;
    assertEquals(max, connect.refresherRequests);
    assertEquals(max + 1, connect.chains.size());
  }

  @Test
  void keepsRequestingOncePerIntervalWhileHealthy() {
    FakeConnect connect = new FakeConnect(Failure.NONE);
    connect.run(TimeUnit.HOURS.toMillis(1));
    long intervals = TimeUnit.HOURS.toMillis(1) / INTERVAL_MS;
    assertTrue(
        connect.refresherRequests >= intervals - 1, "requests: " + connect.refresherRequests);
    assertTrue(connect.refresherRequests <= intervals, "requests: " + connect.refresherRequests);
    assertTrue(connect.chains.isEmpty());
  }

  @Test
  void recoversWhenRequestsAreSkippedBeforeTaskConfigs() {
    FakeConnect connect = new FakeConnect(Failure.NONE);
    // Connect's start request and the next refresher requests never reach taskConfigs().
    connect.skipsLeft = TaskReconfigurationRefresher.MAX_REQUESTS_WITHOUT_SUCCESS;
    connect.run(TimeUnit.HOURS.toMillis(1));
    assertTrue(connect.published);
    assertTrue(connect.chains.isEmpty());
  }

  @Test
  void addsNoRetryChainWhenFailingAfterTaskConfigs() {
    FakeConnect connect = new FakeConnect(Failure.AFTER_TASK_CONFIGS);
    connect.run(TimeUnit.HOURS.toMillis(1));
    assertEquals(0, connect.refresherRequests);
    assertEquals(1, connect.chains.size());
  }

  @Test
  void addsNoRetryChainWhileASlowTaskConfigsCallIsInProgress() {
    FakeConnect connect = new FakeConnect(Failure.AFTER_TASK_CONFIGS);
    connect.taskConfigsMs = 3 * INTERVAL_MS;
    connect.run(TimeUnit.HOURS.toMillis(1));
    assertEquals(0, connect.refresherRequests);
    assertEquals(1, connect.chains.size());
  }

  @Test
  void recoversFromDroppedRequests() {
    FakeConnect connect = new FakeConnect(Failure.NONE);
    connect.dropsLeft = 3;
    connect.run(TimeUnit.HOURS.toMillis(1));
    assertTrue(connect.published);
    assertTrue(connect.chains.isEmpty());
    assertTrue(connect.refresherRequests <= TimeUnit.HOURS.toMillis(1) / INTERVAL_MS);
  }

  @Test
  void retriesEveryIntervalWhileTheRequestThrows() {
    // StandaloneHerder runs taskConfigs() synchronously and has no retry chain of its own.
    AtomicInteger requests = new AtomicInteger();
    long[] now = {0};
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(
            () -> {
              requests.incrementAndGet();
              throw new IllegalStateException("herder not ready");
            },
            INTERVAL_MS,
            () -> now[0]);
    now[0] = TimeUnit.MILLISECONDS.toNanos(INTERVAL_MS);
    refresher.refresh();
    now[0] += TimeUnit.SECONDS.toNanos(1);
    refresher.refresh();
    assertEquals(1, requests.get());
    int max = TaskReconfigurationRefresher.MAX_REQUESTS_WITHOUT_SUCCESS;
    for (int i = 0; i < 2 * max; i++) {
      now[0] += TimeUnit.MILLISECONDS.toNanos(INTERVAL_MS);
      refresher.refresh();
    }
    assertEquals(1 + 2 * max, requests.get());
  }

  @Test
  void doesNotHoldItsLockWhileRequesting() throws Exception {
    // Models StandaloneHerder: requestTaskReconfiguration() runs taskConfigs() under the herder
    // lock, and the master thread can hold that lock while Connect calls taskConfigs() for it.
    Object herderLock = new Object();
    CountDownLatch masterHoldsHerderLock = new CountDownLatch(1);
    CountDownLatch requestStarted = new CountDownLatch(1);
    AtomicReference<TaskReconfigurationRefresher> holder = new AtomicReference<>();
    long[] now = {0};
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(
            () -> {
              requestStarted.countDown();
              synchronized (herderLock) {
                holder.get().trackTaskConfigs(() -> null);
              }
            },
            INTERVAL_MS,
            () -> now[0]);
    holder.set(refresher);
    now[0] = TimeUnit.MILLISECONDS.toNanos(INTERVAL_MS);
    Thread master =
        new Thread(
            () -> {
              synchronized (herderLock) {
                masterHoldsHerderLock.countDown();
                awaitUninterruptibly(requestStarted);
                refresher.trackTaskConfigs(() -> null);
              }
            });
    master.setDaemon(true);
    master.start();
    assertTrue(masterHoldsHerderLock.await(5, TimeUnit.SECONDS));
    Thread refreshing = new Thread(refresher::refresh);
    refreshing.setDaemon(true);
    refreshing.start();
    master.join(5000);
    refreshing.join(5000);
    assertFalse(master.isAlive(), "deadlock between the herder lock and the refresher lock");
    assertFalse(refreshing.isAlive(), "deadlock between the herder lock and the refresher lock");
    assertEquals(0, requestStarted.getCount());
  }

  @Test
  void sendsNothingAfterClose() {
    AtomicInteger requests = new AtomicInteger();
    long[] now = {0};
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(requests::incrementAndGet, INTERVAL_MS, () -> now[0]);
    refresher.close();
    now[0] = TimeUnit.HOURS.toNanos(1);
    refresher.refresh();
    assertEquals(0, requests.get());
  }

  @Test
  void stopsRequestingAfterClose() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    AtomicReference<TaskReconfigurationRefresher> holder = new AtomicReference<>();
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(
            () -> {
              requests.incrementAndGet();
              holder.get().trackTaskConfigs(() -> null);
            },
            10,
            System::nanoTime);
    holder.set(refresher);
    try {
      refresher.start("test");
      awaitAtLeast(requests, 3);
    } finally {
      refresher.close();
    }
    Thread.sleep(50);
    int afterClose = requests.get();
    Thread.sleep(100);
    assertEquals(afterClose, requests.get());
  }

  @Test
  void doesNotInterruptARequestWaitingForTheHerderLock() throws Exception {
    // Models StandaloneHerder.restartConnector(): it stops the connector under the herder lock, and
    // a request waiting for that lock then runs taskConfigs() for the restarted connector.
    Object herderLock = new Object();
    CountDownLatch requestStarted = new CountDownLatch(1);
    CountDownLatch requestDone = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(
            () -> {
              requestStarted.countDown();
              synchronized (herderLock) {
                interrupted.set(Thread.currentThread().isInterrupted());
              }
              requestDone.countDown();
            },
            10,
            System::nanoTime);
    synchronized (herderLock) {
      refresher.start("test");
      assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
      refresher.close();
    }
    assertTrue(requestDone.await(5, TimeUnit.SECONDS));
    assertFalse(interrupted.get(), "close() interrupted a request waiting for the herder lock");
  }

  @Test
  void keepsRunningWhenTheRequestThrowsAnError() throws Exception {
    // StandaloneHerder runs taskConfigs() on the refresher thread, so an Error can reach it.
    AtomicInteger requests = new AtomicInteger();
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(
            () -> {
              requests.incrementAndGet();
              throw new LinkageError("taskConfigs() failed");
            },
            10,
            System::nanoTime);
    try {
      refresher.start("test");
      awaitAtLeast(requests, 2);
    } finally {
      refresher.close();
    }
  }

  @Test
  void intervalDefaultsTo120sAllowsZeroAndRejectsShorterValues() {
    ScyllaConnectorConfig defaults = new ScyllaConnectorConfig(baseConfig().build());
    assertEquals(120000L, defaults.getTaskReconfigurationIntervalMs());
    assertTrue(validate("0").errorMessages().isEmpty());
    assertTrue(validate("120000").errorMessages().isEmpty());
    assertFalse(validate("119999").errorMessages().isEmpty());
    assertFalse(validate("1000").errorMessages().isEmpty());
    assertFalse(validate("-1").errorMessages().isEmpty());
    assertEquals(1, validate("abc").errorMessages().size());
  }

  private enum Failure {
    NONE,
    BEFORE_TASK_CONFIGS,
    IN_TASK_CONFIGS,
    AFTER_TASK_CONFIGS
  }

  /**
   * Models Kafka Connect 3.7 {@code DistributedHerder}: every request starts a chain that retries
   * any failure with a 250 ms backoff doubling up to 60 s (plus any queueing delay), and ends on
   * success or a silent drop.
   */
  private static final class FakeConnect {
    private static final long STEP_MS = 250;

    final List<long[]> chains = new ArrayList<>(); // {dueMs, attempts}
    final TaskReconfigurationRefresher refresher;
    final Failure failure;
    long taskConfigsMs;
    long queueDelayMs;
    int dropsLeft;
    int skipsLeft;
    int failuresLeft = Integer.MAX_VALUE;
    int skipsAfterFailure;
    int refresherRequests;
    boolean published;
    long nowMs;

    FakeConnect(Failure failure) {
      this.failure = failure;
      this.refresher =
          new TaskReconfigurationRefresher(
              () -> {
                refresherRequests++;
                chains.add(new long[] {nowMs, 0});
              },
              INTERVAL_MS,
              () -> TimeUnit.MILLISECONDS.toNanos(nowMs));
      chains.add(new long[] {0, 0}); // Connect's own request on connector start.
    }

    void run(long durationMs) {
      for (long end = nowMs + durationMs; nowMs < end; nowMs += STEP_MS) {
        for (long[] chain : new ArrayList<>(chains)) {
          if (chain[0] <= nowMs && !attempt()) {
            chain[0] = nowMs + Math.min(250L << Math.min(chain[1]++, 8), 60_000L) + queueDelayMs;
          } else if (chain[0] <= nowMs) {
            chains.remove(chain);
          }
        }
        refresher.refresh();
      }
    }

    /** Returns whether the chain ends. */
    private boolean attempt() {
      if (skipsLeft > 0) {
        skipsLeft--; // e.g. "not running" or target state not STARTED: the chain ends silently.
        return true;
      }
      if (failure == Failure.BEFORE_TASK_CONFIGS) {
        return false;
      }
      try {
        refresher.trackTaskConfigs(
            () -> {
              for (long end = nowMs + taskConfigsMs; nowMs < end; nowMs += STEP_MS) {
                refresher.refresh();
              }
              if (failure == Failure.IN_TASK_CONFIGS && failuresLeft-- > 0) {
                throw new IllegalStateException("assignments exceed max bytes");
              }
              return null;
            });
      } catch (IllegalStateException e) {
        skipsLeft += skipsAfterFailure;
        skipsAfterFailure = 0;
        return false;
      }
      if (failure == Failure.AFTER_TASK_CONFIGS) {
        return false;
      }
      if (dropsLeft > 0) {
        dropsLeft--;
      } else {
        published = true;
      }
      return true;
    }
  }

  private static Configuration.Builder baseConfig() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
        .with("scylla.table.names", "ks.table");
  }

  private static ConfigValue validate(String value) {
    String field = "scylla.task.reconfiguration.interval.ms";
    Configuration config = Configuration.create().with(field, value).build();
    return config.validate(ScyllaConnectorConfig.ALL_FIELDS).get(field);
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void awaitAtLeast(AtomicInteger counter, int expected) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (counter.get() < expected) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("expected >= " + expected + " requests, got " + counter.get());
      }
      Thread.sleep(5);
    }
  }
}
