package com.mapr.kafka.eventstreams.impl;
import java.util.*;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.kafka.eventstreams.impl.listener.MarlinListener;
import org.apache.kafka.connect.runtime.distributed.ConnectProtocol;
import org.apache.kafka.connect.runtime.distributed.ExtendedAssignment;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.WorkerRebalanceListener;
import com.mapr.kafka.eventstreams.impl.listener.MarlinListener.MarlinJoinCallback;
import com.mapr.fs.proto.Marlinserver.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.mapr.GenericHFactory;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.distributed.GenericWorkerCoordinator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.connect.util.ConnectorTaskId;

public class MarlinWorkerCoordinator extends MarlinCoordinator implements GenericWorkerCoordinator {

  private LeaderState leaderState;
  private ExtendedAssignment assignmentSnapshot;
  private ClusterConfigState configSnapshot;
  private DistributedConfig config;
  private final String restUrl;
  protected final WorkerRebalanceListener rebalanceCb;
  private final KafkaConfigBackingStore configStorage;

  public MarlinWorkerCoordinator(DistributedConfig conf, String groupId,
                                  String restUrl, KafkaConfigBackingStore configStorage,
                                  WorkerRebalanceListener rebalanceCb) {
    super(groupId);
    this.config = conf;
    this.restUrl = restUrl;
    this.configStorage= configStorage;
    this.rebalanceCb = rebalanceCb;
    this.assignmentSnapshot = null;
    init();
    log.debug("MarlinWorkerCoordinator constructor");
  }

  @Override
  protected Logger getLogger() {
    return LoggerFactory.getLogger(MarlinWorkerCoordinator.class);
  }

  @Override
  protected boolean isProtocolRejoinNeeded() {
    if (assignmentSnapshot != null) {
      log.debug ("assignmentSnapshot.failed {}", assignmentSnapshot.failed());
    }
    return (assignmentSnapshot == null || assignmentSnapshot.failed());
  }

  @Override
  protected void revokeAssignments() {
    log.debug("Revoking previous assignment {}", assignmentSnapshot);
    if (assignmentSnapshot != null && !assignmentSnapshot.failed())
        rebalanceCb.onRevoked(assignmentSnapshot.leader(),
                              assignmentSnapshot.connectors(),
                              assignmentSnapshot.tasks());
  }

  @Override
  public void revokeAssignment(ExtendedAssignment assignment) {
    log.debug("Revoking previous assignment {}", assignment);
    if (assignment != null && !assignment.failed())
      rebalanceCb.onRevoked(assignment.leader(),
              assignment.connectors(),
              assignment.tasks());
  }

  @Override
  protected void protocolOnSyncComplete(MemberState ms, long generationId) {
    ConnectProtocol.Assignment connectProtocolAssignment = ConnectProtocol
        .deserializeAssignment(ms.getMemberAssignment()
            .asReadOnlyByteBuffer());
    assignmentSnapshot = new ExtendedAssignment(
            ConnectProtocol.CONNECT_PROTOCOL_V0, // not sure about protocol version
            connectProtocolAssignment.error(),
            connectProtocolAssignment.leader(),
            connectProtocolAssignment.leaderUrl(),
            connectProtocolAssignment.offset(),
            connectProtocolAssignment.connectors(),
            connectProtocolAssignment.tasks(),
            Collections.emptyList(), Collections.emptyList(), 0);
    // CY TODO: revisit this cast
    invokeAssignCallback(assignmentSnapshot, (int)generationId);
  }

  protected void invokeAssignCallback(ExtendedAssignment assignmentSnapshot, int groupGenerationId) {
    rebalanceCb.onAssigned(assignmentSnapshot, groupGenerationId);
  }

  public short currentProtocolVersion() {
    return assignmentSnapshot.version(); // not sure about protocol version
  }

  @Override
  public void close() {
    super.close();
    assignmentSnapshot = null;
  }


  @Override
  public String ownerUrl(String connector) {
    if (rejoinNeeded() || !isLeader())
      return null;
    return leaderState.ownerUrl(connector);
  }

  @Override
  public String ownerUrl(ConnectorTaskId task) {
    if (rejoinNeeded() || !isLeader())
      return null;
    return leaderState.ownerUrl(task);
  }

  private boolean isLeader() {
    return assignmentSnapshot != null && memberId.equals(assignmentSnapshot.leader());
  }

  @Override
  protected String generateSyncTopic(String groupId) {
    String topic = "__mapr__" + groupId + "_assignment";
    return topic;
  }

  protected String getConfigTopic(Map<String, ?> configs) {
    return (String) configs.get(DistributedConfig.CONFIG_TOPIC_CONFIG);
  }

  @Override
  protected String generateCoordStream() {
    String configTopic = getConfigTopic(config.originals());
    int idx = configTopic.lastIndexOf(':');
    String streamName = configTopic.substring(0, idx);
    return streamName;
  }

  protected ClusterConfigState getConfigSnapshot() {
    return configStorage.snapshot();
  }

  @Override
  protected MarlinJoinCallback getJoinerCallback() {
    return new MarlinWorkerJoinCallback();
  }

  @Override
  protected JoinGroupDesc generateJoinDesc() {
    this.configSnapshot = getConfigSnapshot();
    ConnectProtocol.WorkerState workerState = new ConnectProtocol.WorkerState(this.restUrl, configSnapshot.offset());
    ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
    JoinGroupDesc desc = JoinGroupDesc.newBuilder().setProtocolType("connect")
                                      .setMemberId(this.memberId)
                                      .addMemberProtocols(MemberProtocol.newBuilder()
                                        .setProtocol("default")
                                        .setMemberMetadata(ByteString.copyFrom(metadata)).build())
                                      .build();
    return desc;
  }

  @Override
  protected Map<String, ByteBuffer> performProtocolAssignment(String leaderId, List<Member> members) {
    Map<String, ConnectProtocol.WorkerState> wsMap = new HashMap<>();
    for (Member member : members) {
      wsMap.put(member.getMemberId(),
          ConnectProtocol.deserializeMetadata(member.getMemberMetadata().asReadOnlyByteBuffer()));
    }
    long maxOffset = findMaxMemberConfigOffset(wsMap);
    Long leaderOffset = ensureLeaderConfig(maxOffset);
    if (leaderOffset == null)
      return fillAssignmentsAndSerialize(wsMap.keySet(), ConnectProtocol.Assignment.CONFIG_MISMATCH, leaderId,
          wsMap.get(leaderId).url(), maxOffset, new HashMap<String, List<String>>(),
          new HashMap<String, List<ConnectorTaskId>>());
    return performTaskAssignment(leaderId, leaderOffset, wsMap);
  }

  private long findMaxMemberConfigOffset(Map<String, ConnectProtocol.WorkerState> allConfigs) {
    // The new config offset is the maximum seen by any member. We always
    // perform assignment using this offset,
    // even if some members have fallen behind. The config offset used to
    // generate the assignment is included in
    // the response so members that have fallen behind will not use the
    // assignment until they have caught up.
    Long maxOffset = null;
    for (Map.Entry<String, ConnectProtocol.WorkerState> stateEntry : allConfigs.entrySet()) {
      long memberRootOffset = stateEntry.getValue().offset();
      if (maxOffset == null)
        maxOffset = memberRootOffset;
      else
        maxOffset = Math.max(maxOffset, memberRootOffset);
    }

    log.debug("Max config offset root: {}, local snapshot config offsets root: {}", maxOffset, configSnapshot.offset());
    return maxOffset;
  }

  private Long ensureLeaderConfig(long maxOffset) {
    // If this leader is behind some other members, we can't do assignment
    if (configSnapshot.offset() < maxOffset) {
      // We might be able to take a new snapshot to catch up immediately and
      // avoid another round of syncing here.
      // Alternatively, if this node has already passed the maximum reported by
      // any other member of the group, it
      // is also safe to use this newer state.
      ClusterConfigState updatedSnapshot = getConfigSnapshot();
      if (updatedSnapshot.offset() < maxOffset) {
        log.info("Was selected to perform assignments, but do not have latest config found in sync request. "
            + "Returning an empty configuration to trigger re-sync.");
        return null;
      } else {
        configSnapshot = updatedSnapshot;
        return configSnapshot.offset();
      }
    }

    return maxOffset;
  }

  private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset,
      Map<String, ConnectProtocol.WorkerState> allConfigs) {
    Map<String, List<String>> connectorAssignments = new HashMap<>();
    Map<String, List<ConnectorTaskId>> taskAssignments = new HashMap<>();

    // Perform round-robin task assignment
    CircularIterator<String> memberIt = new CircularIterator<>(Utils.sorted(allConfigs.keySet()));
    for (String connectorId : Utils.sorted(configSnapshot.connectors())) {
      String connectorAssignedTo = memberIt.next();
      log.trace("Assigning connector {} to {}", connectorId, connectorAssignedTo);
      List<String> memberConnectors = connectorAssignments.get(connectorAssignedTo);
      if (memberConnectors == null) {
        memberConnectors = new ArrayList<>();
        connectorAssignments.put(connectorAssignedTo, memberConnectors);
      }
      memberConnectors.add(connectorId);

      for (ConnectorTaskId taskId : Utils.sorted(configSnapshot.tasks(connectorId))) {
        String taskAssignedTo = memberIt.next();
        log.trace("Assigning task {} to {}", taskId, taskAssignedTo);
        List<ConnectorTaskId> memberTasks = taskAssignments.get(taskAssignedTo);
        if (memberTasks == null) {
          memberTasks = new ArrayList<>();
          taskAssignments.put(taskAssignedTo, memberTasks);
        }
        memberTasks.add(taskId);
      }
    }

    leaderState = new LeaderState(allConfigs, connectorAssignments, taskAssignments);

    return fillAssignmentsAndSerialize(allConfigs.keySet(), ConnectProtocol.Assignment.NO_ERROR, leaderId,
        allConfigs.get(leaderId).url(), maxOffset, connectorAssignments, taskAssignments);
  }

  private Map<String, ByteBuffer> fillAssignmentsAndSerialize(Collection<String> members, short error, String leaderId,
      String leaderUrl, long maxOffset, Map<String, List<String>> connectorAssignments,
      Map<String, List<ConnectorTaskId>> taskAssignments) {

    Map<String, ByteBuffer> groupAssignment = new HashMap<>();
    for (String member : members) {
      List<String> connectors = connectorAssignments.get(member);
      if (connectors == null)
        connectors = Collections.emptyList();
      List<ConnectorTaskId> tasks = taskAssignments.get(member);
      if (tasks == null)
        tasks = Collections.emptyList();
      ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(error, leaderId, leaderUrl, maxOffset,
          connectors, tasks);
      log.debug("Assignment: {} -> {}", member, assignment);
      groupAssignment.put(member, ConnectProtocol.serializeAssignment(assignment));
    }
    log.debug("Finished assignment");
    return groupAssignment;
  }

  public class MarlinWorkerJoinCallback extends MarlinCoordinatorJoinCallback {
    @Override
    public void onJoin(JoinGroupInfo jgi) {
      performOnJoin(jgi);
    }
  }

  private static <K, V> Map<V, K> invertAssignment(Map<K, List<V>> assignment) {
    Map<V, K> inverted = new HashMap<>();
    for (Map.Entry<K, List<V>> assignmentEntry : assignment.entrySet()) {
      K key = assignmentEntry.getKey();
      for (V value : assignmentEntry.getValue())
        inverted.put(value, key);
    }
    return inverted;
  }

  private static class LeaderState {
    private final Map<String, ConnectProtocol.WorkerState> allMembers;
    private final Map<String, String> connectorOwners;
    private final Map<ConnectorTaskId, String> taskOwners;

    public LeaderState(Map<String, ConnectProtocol.WorkerState> allMembers,
                       Map<String, List<String>> connectorAssignment,
                       Map<String, List<ConnectorTaskId>> taskAssignment) {
      this.allMembers = allMembers;
      this.connectorOwners = invertAssignment(connectorAssignment);
      this.taskOwners = invertAssignment(taskAssignment);
    }

    private String ownerUrl(ConnectorTaskId id) {
      String ownerId = taskOwners.get(id);
      if (ownerId == null)
          return null;
      return allMembers.get(ownerId).url();
    }

    private String ownerUrl(String connector) {
      String ownerId = connectorOwners.get(connector);
      if (ownerId == null)
          return null;
      return allMembers.get(ownerId).url();
    }

  }

  public boolean ensureCoordinatorReady(final Timer timer) {
    return false;
  }

}
