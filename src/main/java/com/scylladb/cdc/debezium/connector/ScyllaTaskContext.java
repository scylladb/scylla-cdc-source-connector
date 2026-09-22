package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.transport.CoordinationGroup;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.apache.commons.lang3.tuple.Pair;

public class ScyllaTaskContext extends CdcSourceTaskContext {

  private final List<Pair<TaskId, SortedSet<StreamId>>> tasks;
  private final Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups;

  public ScyllaTaskContext(Configuration config, List<Pair<TaskId, SortedSet<StreamId>>> tasks) {
    this(config, tasks, Collections.emptySet());
  }

  public ScyllaTaskContext(
      Configuration config,
      List<Pair<TaskId, SortedSet<StreamId>>> tasks,
      Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups) {
    super(
        Module.contextName(),
        config.getString(CommonConnectorConfig.TOPIC_PREFIX),
        new ScyllaConnectorConfig(config).getCustomMetricTags(),
        Collections::emptySet);
    this.tasks = tasks;
    this.coordinationGroups = Collections.unmodifiableSet(new HashSet<>(coordinationGroups));
  }

  public List<Pair<TaskId, SortedSet<StreamId>>> getTasks() {
    return tasks;
  }

  public Set<CoordinationGroup<TaskId, TaskId>> getCoordinationGroups() {
    return coordinationGroups;
  }
}
