package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.transport.CoordinationGroup;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskConfigBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskConfigBuilder.class);
  private static final int MAX_LOGGED_COORDINATION_GROUPS = 10;

  private static final class SerializedTask {
    private final TaskId taskId;
    private final String value;
    private final int sizeBytes;

    private SerializedTask(TaskId taskId, String value) {
      this.taskId = taskId;
      this.value = value;
      this.sizeBytes = value.getBytes(StandardCharsets.UTF_8).length;
    }
  }

  private static final class SerializedCoordinationGroup {
    private final CoordinationGroup<TaskId, TaskId> coordinationGroup;
    private final String value;
    private final int sizeBytes;

    private SerializedCoordinationGroup(CoordinationGroup<TaskId, TaskId> coordinationGroup) {
      this.coordinationGroup = coordinationGroup;
      this.value = ConfigSerializerUtil.serializeCoordinationGroup(coordinationGroup);
      this.sizeBytes = value.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof SerializedCoordinationGroup)) {
        return false;
      }
      return value.equals(((SerializedCoordinationGroup) other).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  private static final class TaskConfigAccumulator {
    private final int index;
    private final List<SerializedTask> tasks = new ArrayList<>();
    private final Set<SerializedCoordinationGroup> coordinationGroups = new HashSet<>();
    private int sizeBytes;
    private int coordinationSizeBytes;

    private TaskConfigAccumulator(int index) {
      this.index = index;
    }

    private int totalSizeBytes() {
      return sizeBytes + coordinationSizeBytes;
    }

    private long coordinationAffinity(List<SerializedCoordinationGroup> groups) {
      return groups.stream().filter(coordinationGroups::contains).count();
    }

    private boolean canFit(
        SerializedTask task,
        int maxBytes,
        Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
      long additionalBytes = (tasks.isEmpty() ? 0 : 1) + task.sizeBytes;
      for (SerializedCoordinationGroup group :
          coordinationByParticipant.getOrDefault(task.taskId, Collections.emptyList())) {
        if (!coordinationGroups.contains(group)) {
          additionalBytes += 1L + group.sizeBytes;
        }
      }
      return totalSizeBytes() + additionalBytes <= maxBytes;
    }

    private void add(
        SerializedTask task,
        Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
      if (!tasks.isEmpty()) {
        sizeBytes++;
      }
      tasks.add(task);
      sizeBytes += task.sizeBytes;
      coordinationByParticipant
          .getOrDefault(task.taskId, Collections.emptyList())
          .forEach(
              group -> {
                if (coordinationGroups.add(group)) {
                  // Every coordination record follows at least one task record.
                  coordinationSizeBytes += 1 + group.sizeBytes;
                }
              });
    }

    private SerializedTask removeSmallest(
        Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
      SerializedTask task = tasks.remove(tasks.size() - 1);
      sizeBytes -= task.sizeBytes;
      if (!tasks.isEmpty()) {
        sizeBytes--;
      }
      coordinationGroups.clear();
      coordinationSizeBytes = 0;
      tasks.stream()
          .flatMap(
              remaining ->
                  coordinationByParticipant
                      .getOrDefault(remaining.taskId, Collections.emptyList())
                      .stream())
          .forEach(
              group -> {
                if (coordinationGroups.add(group)) {
                  coordinationSizeBytes += 1 + group.sizeBytes;
                }
              });
      return task;
    }

    private String serialize() {
      List<String> lines =
          tasks.stream().map(task -> task.value).collect(Collectors.toCollection(ArrayList::new));
      coordinationGroups.stream().map(group -> group.value).sorted().forEach(lines::add);
      return String.join("\n", lines);
    }
  }

  private final Map<TaskId, SortedSet<StreamId>> tasks;
  private final Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups;
  private final int maxWorkerConfigBytes;

  public TaskConfigBuilder(Map<TaskId, SortedSet<StreamId>> tasks) {
    this(tasks, Collections.emptySet(), ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES);
  }

  TaskConfigBuilder(Map<TaskId, SortedSet<StreamId>> tasks, int maxWorkerConfigBytes) {
    this(tasks, Collections.emptySet(), maxWorkerConfigBytes);
  }

  TaskConfigBuilder(
      Map<TaskId, SortedSet<StreamId>> tasks,
      Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups,
      int maxWorkerConfigBytes) {
    if (maxWorkerConfigBytes <= 0) {
      throw new IllegalArgumentException("Maximum worker configuration size must be positive");
    }
    this.tasks = tasks;
    this.coordinationGroups =
        coordinationGroups == null ? Collections.emptySet() : coordinationGroups;
    this.maxWorkerConfigBytes = maxWorkerConfigBytes;
  }

  public List<String> buildTaskConfigs(int maxTasks) {
    if (maxTasks <= 0) {
      throw new IllegalArgumentException("Maximum task count must be positive");
    }
    if (tasks == null) {
      return Collections.emptyList();
    }

    List<SerializedTask> serializedTasks =
        tasks.entrySet().stream()
            .map(
                t -> {
                  TaskId taskId = t.getKey();
                  SortedSet<StreamId> streamIds = t.getValue();
                  return new SerializedTask(
                      taskId, ConfigSerializerUtil.serializeTaskConfig(taskId, streamIds));
                })
            .collect(Collectors.toList());

    if (serializedTasks.isEmpty()) {
      return Collections.emptyList();
    }

    int taskConfigCount = Math.min(serializedTasks.size(), maxTasks);
    serializedTasks.sort(serializedTaskComparator());
    Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant =
        buildCoordinationIndex();

    List<TaskConfigAccumulator> taskConfigAccumulators =
        pack(serializedTasks, taskConfigCount, coordinationByParticipant);
    if (taskConfigAccumulators == null) {
      taskConfigAccumulators =
          packWithoutUnfittableCoordination(
              serializedTasks, taskConfigCount, coordinationByParticipant);
    }

    return taskConfigAccumulators.stream()
        .map(TaskConfigAccumulator::serialize)
        .collect(Collectors.toList());
  }

  /**
   * Packs every task into {@code taskConfigCount} worker configurations, or returns {@code null}
   * when no layout stays below the byte ceiling.
   */
  private List<TaskConfigAccumulator> pack(
      List<SerializedTask> serializedTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    // Least-loaded placement gives predictable parallelism and balanced task sizes. It spreads the
    // tasks of a table over every configuration, which also copies that table's manifest into each
    // of them.
    List<TaskConfigAccumulator> taskConfigAccumulators =
        packByLeastLoaded(serializedTasks, taskConfigCount, coordinationByParticipant);
    if (fitsByteCeiling(taskConfigAccumulators)) {
      return taskConfigAccumulators;
    }

    // Only once that crosses the byte ceiling is it worth trading spread for bytes: confine each
    // manifest to its own share of the configurations, which is the cheapest layout in bytes.
    if (!coordinationByParticipant.isEmpty()) {
      taskConfigAccumulators =
          packByCoordinationShares(serializedTasks, taskConfigCount, coordinationByParticipant);
      if (fitsByteCeiling(taskConfigAccumulators)) {
        return taskConfigAccumulators;
      }
    }

    taskConfigAccumulators =
        packByBestFit(serializedTasks, taskConfigCount, coordinationByParticipant);
    // Individual task sizes can expose small groups before a much larger manifest. Retry with
    // whole coordination groups ordered by footprint before concluding that no packing exists.
    if (taskConfigAccumulators == null && !coordinationByParticipant.isEmpty()) {
      taskConfigAccumulators =
          packByBestFit(
              orderByCoordinationFootprint(serializedTasks, coordinationByParticipant),
              taskConfigCount,
              coordinationByParticipant);
    }
    return taskConfigAccumulators;
  }

  private boolean fitsByteCeiling(List<TaskConfigAccumulator> taskConfigAccumulators) {
    return taskConfigAccumulators.stream()
        .noneMatch(config -> config.totalSizeBytes() > maxWorkerConfigBytes);
  }

  /**
   * Packs again without the coordination manifests that do not fit, or throws when even the tasks
   * alone cannot be packed.
   *
   * <p>A manifest only lets the library retire legacy tablet checkpoints early; an assignment
   * without one keeps those checkpoints until every replacement task has stored its own state.
   * Dropping a manifest therefore defers migration bookkeeping, which is preferable to leaving the
   * connector unable to produce any configuration for the table.
   */
  private List<TaskConfigAccumulator> packWithoutUnfittableCoordination(
      List<SerializedTask> serializedTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    if (coordinationByParticipant.isEmpty()) {
      throw oversizedConfiguration(taskConfigCount, serializedTasks);
    }

    List<TaskConfigAccumulator> taskConfigAccumulators = null;
    Set<SerializedCoordinationGroup> droppedGroups =
        unfittableCoordinationGroups(serializedTasks, coordinationByParticipant);
    if (!droppedGroups.isEmpty()) {
      taskConfigAccumulators =
          pack(
              serializedTasks,
              taskConfigCount,
              withoutCoordinationGroups(coordinationByParticipant, droppedGroups));
    }
    if (taskConfigAccumulators == null) {
      // The manifests fit one at a time, but not together with the tasks in the available
      // configurations. Keep the assignments and give up every manifest instead.
      droppedGroups =
          coordinationByParticipant.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      taskConfigAccumulators = pack(serializedTasks, taskConfigCount, Collections.emptyMap());
    }
    if (taskConfigAccumulators == null) {
      throw oversizedConfiguration(taskConfigCount, serializedTasks);
    }

    LOGGER.warn(
        "Dropped {} tablet coordination manifest(s) from the worker configurations, because they do"
            + " not fit into {} configuration(s) of at most scylla.worker.config.max.bytes={}"
            + " bytes: {}. The legacy tablet checkpoints of those tables are kept until every"
            + " replacement task has stored its own offset. Increase tasks.max or the byte limit to"
            + " keep the manifests.",
        droppedGroups.size(),
        taskConfigCount,
        maxWorkerConfigBytes,
        describe(droppedGroups));
    return taskConfigAccumulators;
  }

  /**
   * Returns the groups whose manifest cannot share a configuration with even their smallest
   * participant, and which therefore cannot be shipped at any {@code tasks.max}.
   */
  private Set<SerializedCoordinationGroup> unfittableCoordinationGroups(
      List<SerializedTask> serializedTasks,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    Map<SerializedCoordinationGroup, Integer> smallestParticipants = new HashMap<>();
    serializedTasks.forEach(
        task ->
            coordinationByParticipant
                .getOrDefault(task.taskId, Collections.emptyList())
                .forEach(group -> smallestParticipants.merge(group, task.sizeBytes, Math::min)));
    return smallestParticipants.entrySet().stream()
        .filter(entry -> entry.getKey().sizeBytes + 1L + entry.getValue() > maxWorkerConfigBytes)
        .map(Map.Entry::getKey)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static Map<TaskId, List<SerializedCoordinationGroup>> withoutCoordinationGroups(
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant,
      Set<SerializedCoordinationGroup> droppedGroups) {
    Map<TaskId, List<SerializedCoordinationGroup>> result = new HashMap<>();
    coordinationByParticipant.forEach(
        (participant, groups) -> {
          List<SerializedCoordinationGroup> retained =
              groups.stream()
                  .filter(group -> !droppedGroups.contains(group))
                  .collect(Collectors.toList());
          if (!retained.isEmpty()) {
            result.put(participant, retained);
          }
        });
    return result;
  }

  private static String describe(Set<SerializedCoordinationGroup> groups) {
    String keys =
        groups.stream()
            .map(group -> group.coordinationGroup.getKey().toString())
            .sorted()
            .limit(MAX_LOGGED_COORDINATION_GROUPS)
            .collect(Collectors.joining(", "));
    return groups.size() > MAX_LOGGED_COORDINATION_GROUPS
        ? keys + ", ... (" + (groups.size() - MAX_LOGGED_COORDINATION_GROUPS) + " more)"
        : keys;
  }

  private Map<TaskId, List<SerializedCoordinationGroup>> buildCoordinationIndex() {
    Map<TaskId, List<SerializedCoordinationGroup>> result = new HashMap<>();
    coordinationGroups.forEach(
        group -> {
          SerializedCoordinationGroup serialized = new SerializedCoordinationGroup(group);
          group.getParticipants().stream()
              .filter(tasks::containsKey)
              .forEach(
                  participant ->
                      result
                          .computeIfAbsent(participant, ignored -> new ArrayList<>())
                          .add(serialized));
        });
    return result;
  }

  private static List<SerializedTask> orderByCoordinationFootprint(
      List<SerializedTask> serializedTasks,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    Map<SerializedCoordinationGroup, List<SerializedTask>> tasksByGroup = new HashMap<>();
    serializedTasks.forEach(
        task ->
            coordinationByParticipant
                .getOrDefault(task.taskId, Collections.emptyList())
                .forEach(
                    group ->
                        tasksByGroup
                            .computeIfAbsent(group, ignored -> new ArrayList<>())
                            .add(task)));

    List<SerializedCoordinationGroup> groups = new ArrayList<>(tasksByGroup.keySet());
    groups.sort(
        Comparator.comparingLong(
                (SerializedCoordinationGroup group) ->
                    coordinationFootprint(group, tasksByGroup.get(group)))
            .reversed()
            .thenComparing(group -> group.value));

    Set<SerializedTask> orderedTasks = new LinkedHashSet<>();
    groups.forEach(
        group ->
            tasksByGroup.get(group).stream()
                .sorted(serializedTaskComparator())
                .forEach(orderedTasks::add));
    serializedTasks.stream()
        .filter(task -> !orderedTasks.contains(task))
        .forEach(orderedTasks::add);
    return new ArrayList<>(orderedTasks);
  }

  private static long coordinationFootprint(
      SerializedCoordinationGroup group, List<SerializedTask> tasks) {
    return group.sizeBytes + tasks.stream().mapToLong(task -> 1L + task.sizeBytes).sum();
  }

  private static Comparator<SerializedTask> serializedTaskComparator() {
    return Comparator.comparingInt((SerializedTask task) -> task.sizeBytes)
        .reversed()
        .thenComparing(task -> task.value);
  }

  /** Spreads every task over all configurations, ignoring which manifests they carry. */
  private static List<TaskConfigAccumulator> packByLeastLoaded(
      List<SerializedTask> serializedTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    return packShares(
        Collections.singletonList(serializedTasks), taskConfigCount, coordinationByParticipant);
  }

  /**
   * Places the tasks least-loaded within their own share of the configurations, so that a manifest
   * is copied into the configurations of its share instead of all of them. This costs the fewest
   * bytes, at the price of spreading a table's tasks over fewer Kafka Connect tasks.
   */
  private static List<TaskConfigAccumulator> packByCoordinationShares(
      List<SerializedTask> serializedTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    return packShares(
        partitionByCoordination(serializedTasks, taskConfigCount, coordinationByParticipant),
        taskConfigCount,
        coordinationByParticipant);
  }

  private static List<TaskConfigAccumulator> packShares(
      List<List<SerializedTask>> shareTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    List<TaskConfigAccumulator> accumulators = new ArrayList<>(taskConfigCount);
    for (int i = 0; i < taskConfigCount; i++) {
      accumulators.add(new TaskConfigAccumulator(i));
    }

    int[] shares = allocateConfigShares(shareTasks, taskConfigCount);
    int firstConfig = 0;
    for (int share = 0; share < shareTasks.size(); share++) {
      fillByLeastLoaded(
          shareTasks.get(share),
          accumulators.subList(firstConfig, firstConfig + shares[share]),
          coordinationByParticipant);
      firstConfig += shares[share];
    }
    return accumulators;
  }

  private static void fillByLeastLoaded(
      List<SerializedTask> serializedTasks,
      List<TaskConfigAccumulator> accumulators,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    PriorityQueue<TaskConfigAccumulator> leastLoaded =
        new PriorityQueue<>(
            Comparator.comparingInt(TaskConfigAccumulator::totalSizeBytes)
                .thenComparingInt(config -> config.tasks.size())
                .thenComparingInt(config -> config.index));
    leastLoaded.addAll(accumulators);
    for (SerializedTask serializedTask : serializedTasks) {
      TaskConfigAccumulator accumulator = leastLoaded.remove();
      accumulator.add(serializedTask, coordinationByParticipant);
      leastLoaded.add(accumulator);
    }
  }

  /**
   * Splits the tasks into shares that carry the same manifests, so that least-loaded placement
   * copies every manifest into as few configurations as possible. Tasks without a manifest form
   * their own share, and shares are merged when they outnumber the available configurations.
   */
  private static List<List<SerializedTask>> partitionByCoordination(
      List<SerializedTask> serializedTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    if (coordinationByParticipant.isEmpty() || taskConfigCount == 1) {
      return Collections.singletonList(serializedTasks);
    }

    Map<List<String>, List<SerializedTask>> tasksByManifests = new LinkedHashMap<>();
    serializedTasks.forEach(
        task ->
            tasksByManifests
                .computeIfAbsent(
                    manifestSignature(task, coordinationByParticipant),
                    ignored -> new ArrayList<>())
                .add(task));

    List<List<SerializedTask>> shareTasks = new ArrayList<>(tasksByManifests.values());
    return shareTasks.size() <= taskConfigCount
        ? shareTasks
        : mergeToConfigCount(shareTasks, taskConfigCount);
  }

  private static List<String> manifestSignature(
      SerializedTask task,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    return coordinationByParticipant.getOrDefault(task.taskId, Collections.emptyList()).stream()
        .map(group -> group.value)
        .sorted()
        .collect(Collectors.toList());
  }

  /** Merges the shares into as many groups as there are configurations, by descending bytes. */
  private static List<List<SerializedTask>> mergeToConfigCount(
      List<List<SerializedTask>> shareTasks, int taskConfigCount) {
    long[] shareBytes = new long[shareTasks.size()];
    Integer[] largestFirst = new Integer[shareTasks.size()];
    for (int share = 0; share < shareTasks.size(); share++) {
      shareBytes[share] = totalTaskBytes(shareTasks.get(share));
      largestFirst[share] = share;
    }
    Arrays.sort(
        largestFirst, Comparator.comparingLong((Integer share) -> shareBytes[share]).reversed());

    List<List<SerializedTask>> merged = new ArrayList<>(taskConfigCount);
    long[] mergedBytes = new long[taskConfigCount];
    for (int i = 0; i < taskConfigCount; i++) {
      merged.add(new ArrayList<>());
    }
    for (int share : largestFirst) {
      int lightest = 0;
      for (int candidate = 1; candidate < taskConfigCount; candidate++) {
        if (mergedBytes[candidate] < mergedBytes[lightest]) {
          lightest = candidate;
        }
      }
      merged.get(lightest).addAll(shareTasks.get(share));
      mergedBytes[lightest] += shareBytes[share];
    }
    merged.forEach(mergedTasks -> mergedTasks.sort(serializedTaskComparator()));
    return merged;
  }

  /**
   * Hands every configuration to a share, one each and the rest to whichever share would otherwise
   * carry the most bytes per configuration. No share receives more configurations than it has
   * tasks, so every configuration ends up with work.
   */
  private static int[] allocateConfigShares(
      List<List<SerializedTask>> shareTasks, int taskConfigCount) {
    int[] shares = new int[shareTasks.size()];
    Arrays.fill(shares, 1);
    long[] shareBytes = new long[shareTasks.size()];
    for (int share = 0; share < shareTasks.size(); share++) {
      shareBytes[share] = totalTaskBytes(shareTasks.get(share));
    }

    for (int remaining = taskConfigCount - shareTasks.size(); remaining > 0; remaining--) {
      int widest = -1;
      double widestBytesPerConfig = -1.0;
      for (int share = 0; share < shareTasks.size(); share++) {
        if (shares[share] >= shareTasks.get(share).size()) {
          continue;
        }
        double bytesPerConfig = (double) shareBytes[share] / (shares[share] + 1);
        if (bytesPerConfig > widestBytesPerConfig) {
          widestBytesPerConfig = bytesPerConfig;
          widest = share;
        }
      }
      if (widest < 0) {
        // Unreachable: the shares hold at least taskConfigCount tasks in total.
        throw new IllegalStateException();
      }
      shares[widest]++;
    }
    return shares;
  }

  private static long totalTaskBytes(List<SerializedTask> serializedTasks) {
    return serializedTasks.stream().mapToLong(task -> 1L + task.sizeBytes).sum();
  }

  private List<TaskConfigAccumulator> packByBestFit(
      List<SerializedTask> serializedTasks,
      int taskConfigCount,
      Map<TaskId, List<SerializedCoordinationGroup>> coordinationByParticipant) {
    List<TaskConfigAccumulator> taskConfigAccumulators = new ArrayList<>(taskConfigCount);
    for (SerializedTask serializedTask : serializedTasks) {
      List<SerializedCoordinationGroup> taskCoordinationGroups =
          coordinationByParticipant.getOrDefault(serializedTask.taskId, Collections.emptyList());
      TaskConfigAccumulator accumulator =
          taskConfigAccumulators.stream()
              .filter(
                  config ->
                      config.canFit(
                          serializedTask, maxWorkerConfigBytes, coordinationByParticipant))
              .max(
                  Comparator.comparingLong(
                          (TaskConfigAccumulator config) ->
                              config.coordinationAffinity(taskCoordinationGroups))
                      .thenComparingInt(TaskConfigAccumulator::totalSizeBytes)
                      .thenComparingInt(config -> -config.index))
              .orElse(null);
      // Keep a new manifest out of records already assigned to other coordination groups while an
      // unused record is available. Subsequent participants then prefer that seeded record.
      if (accumulator == null
          || (!taskCoordinationGroups.isEmpty()
              && accumulator.coordinationAffinity(taskCoordinationGroups) == 0
              && taskConfigAccumulators.size() < taskConfigCount)) {
        if (taskConfigAccumulators.size() >= taskConfigCount
            || !new TaskConfigAccumulator(0)
                .canFit(serializedTask, maxWorkerConfigBytes, coordinationByParticipant)) {
          return null;
        }
        accumulator = new TaskConfigAccumulator(taskConfigAccumulators.size());
        taskConfigAccumulators.add(accumulator);
      }
      accumulator.add(serializedTask, coordinationByParticipant);
    }

    // Best-fit packing can use fewer records than Kafka Connect made available. Split packed
    // records without changing their byte safety so taskConfigs() returns exactly min(S, maxTasks).
    while (taskConfigAccumulators.size() < taskConfigCount) {
      TaskConfigAccumulator donor =
          taskConfigAccumulators.stream()
              .filter(config -> config.tasks.size() > 1)
              .max(
                  Comparator.comparingInt((TaskConfigAccumulator config) -> config.tasks.size())
                      .thenComparingInt(TaskConfigAccumulator::totalSizeBytes)
                      .thenComparingInt(config -> -config.index))
              .orElseThrow(IllegalStateException::new);
      TaskConfigAccumulator split = new TaskConfigAccumulator(taskConfigAccumulators.size());
      split.add(donor.removeSmallest(coordinationByParticipant), coordinationByParticipant);
      taskConfigAccumulators.add(split);
    }

    return taskConfigAccumulators;
  }

  private ConnectException oversizedConfiguration(
      int availableTaskConfigs, List<SerializedTask> serializedTasks) {
    SerializedTask largestTask =
        serializedTasks.stream()
            .max(Comparator.comparingInt(task -> task.sizeBytes))
            .orElseThrow(IllegalStateException::new);
    if (largestTask.sizeBytes > maxWorkerConfigBytes) {
      return new ConnectException(
          String.format(
              "The stream assignment of task %s takes %,d bytes on its own, which exceeds "
                  + "scylla.worker.config.max.bytes=%,d. A single task assignment is never split, "
                  + "so neither tasks.max nor splitting the configured tables across connectors "
                  + "helps here. Raise the byte limit only after increasing Kafka's producer and "
                  + "config-topic record limits.",
              largestTask.taskId, largestTask.sizeBytes, maxWorkerConfigBytes));
    }
    return new ConnectException(
        String.format(
            "Unable to safely pack the stream assignments into %d Kafka Connect task "
                + "configurations without exceeding scylla.worker.config.max.bytes=%,d. Increase "
                + "tasks.max or split the configured tables across connectors. Raise the byte "
                + "limit only after increasing Kafka's producer and config-topic record limits.",
            availableTaskConfigs, maxWorkerConfigBytes));
  }
}
