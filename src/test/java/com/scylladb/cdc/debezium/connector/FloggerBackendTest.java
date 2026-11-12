package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.backend.Platform;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class FloggerBackendTest {

  @Test
  void discoveredSlf4jBackendEmitsMessagesAndCauses() throws InterruptedException {
    assertEquals(
        "com.google.common.flogger.backend.slf4j.Slf4jLoggerBackend",
        Platform.getBackend(FloggerBackendTest.class.getName()).getClass().getName());
    Logger logger = Logger.getLogger(FloggerBackendTest.class.getName());
    Level originalLevel = logger.getLevel();
    BlockingQueue<LogRecord> captured = new ArrayBlockingQueue<>(1);
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            captured.offer(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    logger.setLevel(Level.WARNING);
    try {
      RuntimeException cause = new RuntimeException("test cause");
      FluentLogger.forEnclosingClass().atWarning().withCause(cause).log("Flogger %s", "message");
      // Another test may still be initializing SLF4J and replay this warning later.
      LogRecord event = captured.poll(10, TimeUnit.SECONDS);
      assertNotNull(event, "Flogger warning was not delivered");
      assertEquals("Flogger message", event.getMessage());
      assertEquals(Level.WARNING, event.getLevel());
      assertSame(cause, event.getThrown());
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      handler.close();
    }
  }
}
