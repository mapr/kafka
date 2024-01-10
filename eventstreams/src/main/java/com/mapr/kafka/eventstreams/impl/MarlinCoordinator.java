package com.mapr.kafka.eventstreams.impl;

import java.util.*;
import java.nio.ByteBuffer;

import com.mapr.kafka.eventstreams.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.kafka.eventstreams.impl.listener.MarlinListener;
import org.apache.kafka.connect.runtime.distributed.ConnectProtocol;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.WorkerRebalanceListener;
import com.mapr.kafka.eventstreams.impl.listener.MarlinListener.MarlinJoinCallback;
import com.mapr.fs.proto.Marlinserver.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.ByteString;
import com.mapr.kafka.eventstreams.Streams;
import com.mapr.kafka.eventstreams.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.mapr.GenericHFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.connect.util.ConnectorTaskId;

public abstract class MarlinCoordinator {
  protected static Logger log = LoggerFactory.getLogger(MarlinCoordinator.class);
  private MarlinListener joiner;
  private final String groupId;
  private String syncTopic;
  private String coordStream;
  private String streamTopic;
  protected Long groupGenerationId;
  private static final String UNKNOWN_MEMBER_ID_STR = "";
  protected String memberId;

  private KafkaConsumer<Long, byte[]> syncReceiver;
  protected KafkaProducer<Long, byte[]> syncProducer;
  private static final long SYNC_POLL_TIMEOUT = 15000;
  private ClusterConfigState configSnapshot;

  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private boolean joinComplete;
  protected boolean rejoinEvent;
  private boolean wakeupEvent;
  private boolean isRejoinRequested;
  private boolean needsJoinPrepare;
  private MarlinJoinCallback joinerCallback;
  private long backoffTimeMs;
  private static final int kMaxBackoffTimeMs = 900000;

  public MarlinCoordinator(String groupId) {
    this.groupId = groupId;
    groupGenerationId = 0L;
    this.memberId = UNKNOWN_MEMBER_ID_STR;
    joinComplete = false;
    rejoinEvent = false;
    wakeupEvent = false;
    isRejoinRequested = false;

    needsJoinPrepare = true;
    resetBackoff();
    log.debug("MarlinCoordinator constructor");
  }

  protected void init() {
    syncTopic = generateSyncTopic(this.groupId);
    coordStream = generateCoordStream();
    joiner = getJoiner(groupId, coordStream);
    initSync();
    joinerCallback = getJoinerCallback();
    log = getLogger();
  }

  private MarlinListener<?, ?> getJoiner(String groupId, String coordStream) {
    Properties props = new Properties();
    props.put("group.id", groupId);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("streams.consumer.default.stream", coordStream);
    // Disable the streams.clientside.partition.assignment to remove recursive
    // loop.
    props.put("streams.clientside.partition.assignment", "false");
    ConsumerConfig config = GenericHFactory.getImplementorInstance("org.apache.kafka.clients.consumer.ConsumerConfig",
        new Object[] { props }, new Class[] { Map.class });
    MarlinListener<byte[], byte[]> marlinListener = new MarlinListener<>(config, null, null);
    return marlinListener;
  }

  private void initSync() {
    // Make sure syncTopic is present.
    log.debug("Creating Sync topic {} : ", syncTopic);

    try {
      Admin madmin = Streams.newAdmin(new Configuration());
      try {
        madmin.createTopic(coordStream, syncTopic, 1);
      } catch (Exception e) {
        log.debug("Sync topic creation failed : {} ", e.getMessage());
      } finally {
        madmin.close();
      }
    } catch (Exception e){
      log.debug("Sync topic creation. Failed to create admin client: {} ", e.getMessage());
    }

    streamTopic = coordStream + ":" + syncTopic;

    Properties props = new Properties();
    props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("auto.offset.reset", "latest");
    props.put("streams.clientside.partition.assignment", "false");
    syncReceiver = new KafkaConsumer<Long, byte[]>(props);
    syncProducer = new KafkaProducer<Long, byte[]>(props);
    TopicPartition tp = new TopicPartition(streamTopic, 0);
    syncReceiver.assign(Arrays.asList(tp));
  }

  protected abstract Logger getLogger();

  protected abstract MarlinJoinCallback getJoinerCallback();

  protected abstract String generateSyncTopic(String groupId);

  protected abstract String generateCoordStream();

  protected abstract JoinGroupDesc generateJoinDesc();

  protected abstract void revokeAssignments();

  protected abstract void protocolOnSyncComplete(MemberState ms, long generationId);

  protected abstract Map<String, ByteBuffer> performProtocolAssignment(String leaderId, List<Member> members);

  protected void onSyncComplete(GroupAssignment ga) {
    log.debug("ga gen id {}", ga.getGroupGenerationId());
    for (MemberState ms : ga.getMemberStateList()) {
      log.debug("ms id {} ", ms.getMemberId());
      if (this.memberId() != null) {
        log.debug("this id {}", this.memberId());
      }
      if (ms.getMemberId().equals(this.memberId())) {
        // Perform protocol specific on-sync actions.
        protocolOnSyncComplete(ms, ga.getGroupGenerationId());
        needsJoinPrepare = true;
      }
    }
  }

  // runs two phases namely JOIN and SYNC
  // JOIN uses an RPC and SYNC uses Kafka Producer and Consumer
  // to synchronize all workers.
  // After the JOIN phase is inititated, we need to wait for its
  // completion before proceeding to the SYNC phase. During the wait,
  // if a re-join is requried (say because another member joined),
  // then we re-inititate the JOIN phase.
  public void ensureActiveGroup() {
    while (rejoinNeeded()) {

      resetRejoinFlags();

      if (needsJoinPrepare) {
        // Perform protocol specfic revokes.
        revokeAssignments();
        needsJoinPrepare = false;
      }

      JoinGroupDesc desc = generateJoinDesc();
      joinComplete = false;
      JoinGroupResponse resp = joiner.join(desc, joinerCallback);
      log.debug("ensureActiveGroup: joinStatus {}", resp.getJoinStatus());
      handleJoinGroupResponse(resp);

      waitForJoinOrRejoinEvent();

      if (joinComplete) {
        doSync();
      }
    }
  }

  private void handleJoinGroupResponse(JoinGroupResponse resp) {
    switch (resp.getJoinStatus()) {
    case UNKNOWN_MEMBER_ID:
      this.memberId = "";
      lock.lock();
      rejoinEvent = true;
      lock.unlock();
      break;

    case STATUS_OK:
      this.memberId = resp.getMemberId();
      resetBackoff();
      break;

    case FUNCTION_UNAVAILABLE:
      throw new BrokerNotAvailableException(
          "Feature not available on server." + " Please upgrade to at least Version 5.2.1");

    case STREAM_AUTHORIZATION_FAILED:
      throw new AuthorizationException("Need produceperm and consumeperm permissions on stream " + coordStream);

    case STREAM_UNAVAILABLE:
      log.error("Could not open stream " + coordStream);
      // fall through
    default:
      log.error("Join Group request failed with {}. Retrying with exponential backoff", resp.getJoinStatus());
      backoff();
      break;
    }
  }

  private void backoff() {
    if (backoffTimeMs * 2 + 1000 < kMaxBackoffTimeMs)
      backoffTimeMs = backoffTimeMs * 2 + 1000;
    else
      backoffTimeMs = kMaxBackoffTimeMs;

    Utils.sleep(backoffTimeMs);
  }

  private void resetBackoff() {
    backoffTimeMs = 0;
  }

  private void waitForJoinOrRejoinEvent() {
    log.debug("waitForJoinOrRejoinEvent: memberId {}start", this.memberId);
    try {
      lock.lock();
      while (joinComplete == false && rejoinEvent == false) {
        condition.await();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      log.debug("waitForJoinOrRejoinEvent: memberId {} interrupted", this.memberId);
    } finally {
      lock.unlock();
    }
    log.debug("waitForJoinOrRejoinEvent: memberId {} awoken. joinComplete {} rejoinEvent {}", this.memberId,
        joinComplete, rejoinEvent);
  }

  private void doSync() {
    int i = 0;
    while (true) {
      ConsumerRecords<Long, byte[]> records = syncReceiver.poll(SYNC_POLL_TIMEOUT);
      log.debug("doSync: memberId {} returned from poll {}", this.memberId, i);
      Long lastSeen = 0L;
      for (ConsumerRecord<Long, byte[]> record : records) {
        log.debug("doSync: consumer record..generation ID {}", record.key());
        lastSeen = record.key();
        if (groupGenerationIdMatches(lastSeen)) {
          GroupAssignment ga;
          try {
            ga = GroupAssignment.parseFrom(record.value());
          } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new KafkaException("Error parsing Sync response");
          }
          onSyncComplete(ga);
          return;
        }
      }
      if (rejoinEventOccured() == true)
        return;
      i++;
    }
  }

  private boolean groupGenerationIdMatches(Long lastSeen) {
    lock.lock();
    boolean matches = groupGenerationId.equals(lastSeen);
    lock.unlock();
    return matches;
  }

  public void requestRejoin() {
    log.debug("requestRejoin");
    lock.lock();
    isRejoinRequested = true;
    condition.signal();
    lock.unlock();
  }

  public String memberId() {
    return memberId;
  }

  protected void close() {
    syncReceiver.close();
    syncProducer.close();
    joiner.close();
    // No need to invoke revokeAssignments.
    // Assignments have already been revoked by DistributedHerder.halt()
    lock.lock();
    isRejoinRequested = false;
    rejoinEvent = false;
    lock.unlock();
    // TODO CY: Leave_group RPC
  }

  /*
   * Present to ensure compatibility with MEP 4.1
   */
  public void ensureCoordinatorKnown() {
  }

  public void ensureCoordinatorReady() {
  }

  /*
   * Polls for an event (rejoin/ requestRejoin or wakeup). Note: This does not
   * ensure active group.
   */
  public void pollEvent(long timeout) throws WakeupException {
    log.debug("Poll timeout {}", timeout);
    lock.lock();
    try {
      while (isRejoinRequested == false && rejoinEvent == false && wakeupEvent == false) {
        boolean continueWaiting = condition.await(timeout, TimeUnit.MILLISECONDS);
        if (continueWaiting == false) {
          log.debug("MarlinConsumerCoordinator: poll time expired");
          return;
        }
      }
      if (wakeupEvent == true) {
        wakeupEvent = false;
        log.debug("MarlinConsumerCoordinator: woken up");
        return;
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      log.debug("exiting poll");
      lock.unlock();
    }

    log.debug("exiting poll");
  }

  // throws WakeUpException
  public void poll(long timeout) throws WakeupException {
    log.debug("poll timeout {}", timeout);
    ensureActiveGroup();

    lock.lock();
    try {
      while (isRejoinRequested == false && rejoinEvent == false && wakeupEvent == false) {
        boolean continueWaiting = condition.await(timeout, TimeUnit.MILLISECONDS);
        if (continueWaiting == false) {
          log.debug("MarlinCoordinator: poll time expired");
          return;
        }
      }
      if (wakeupEvent == true) {
        wakeupEvent = false;
        throw new WakeupException();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      log.debug("exiting poll");
      lock.unlock();
    }

    log.debug("exiting poll");
  }

  public void wakeup() {
    log.debug("wakeup: waking up");
    lock.lock();
    wakeupEvent = true;
    condition.signal();
    lock.unlock();
  }

  public void maybeLeaveGroup() {
    requestRejoin();
  }

  public RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
    log.info("Member {} sending LeaveGroup request to coordinator {} due to {}",
            this.memberId, this.groupId, leaveReason);
    maybeLeaveGroup();
    return null;
  }

  protected abstract boolean isProtocolRejoinNeeded();

  protected boolean rejoinNeeded() {
    boolean isRejoinNeeded;
    lock.lock();
    isRejoinNeeded = (isRejoinRequested || rejoinEvent || isProtocolRejoinNeeded());
    lock.unlock();
    log.debug("isRejoinRequested {} rejoinEvent {} ", isRejoinRequested, rejoinEvent);
    return isRejoinNeeded;
  }

  protected void resetRejoinFlags() {
    lock.lock();
    isRejoinRequested = rejoinEvent = false;
    lock.unlock();
  }

  protected boolean rejoinEventOccured() {
    boolean eventOccured;
    lock.lock();
    eventOccured = rejoinEvent;
    lock.unlock();
    return eventOccured;
  }

  protected void performOnJoin(JoinGroupInfo jgi) {
    String leaderId = jgi.getGroupLeaderId();
    log.debug("onJoin: memberId {} leaderId {}", MarlinCoordinator.this.memberId(), leaderId);
    if (leaderId.equals(MarlinCoordinator.this.memberId())) {
      try {
        // Perform protocol specific assignments.
        Map<String, ByteBuffer> assignments = performProtocolAssignment(leaderId, jgi.getMembersList());

        GroupAssignment.Builder gaBuilder = GroupAssignment.newBuilder()
            .setGroupGenerationId(jgi.getGroupGenerationId());
        for (Map.Entry<String, ByteBuffer> e : assignments.entrySet()) {
          log.debug("setting memberstate member id to {}", e.getKey());
          MemberState ms = MemberState.newBuilder().setMemberId(e.getKey())
              .setMemberAssignment(ByteString.copyFrom(e.getValue())).build();
          gaBuilder.addMemberState(ms);
        }

        GroupAssignment ga = gaBuilder.build();
        log.debug("onJoin: memberId {} producing assignment for generation {}", MarlinCoordinator.this.memberId(),
            jgi.getGroupGenerationId());
        syncProducer
            .send(new ProducerRecord<Long, byte[]>(streamTopic, 0, null, jgi.getGroupGenerationId(), ga.toByteArray()));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    lock.lock();
    MarlinCoordinator.this.groupGenerationId = jgi.getGroupGenerationId();
    joinComplete = true;
    condition.signal();
    lock.unlock();
  }

  protected abstract class MarlinCoordinatorJoinCallback implements MarlinJoinCallback {

    /*
     * Do not hold onJoin for too long as it is called in the heartbeat thread
     * context. Let the protocols handle it.
     *
     * performOnJoin needs to be called to continue to group join process.
     */
    @Override
    public abstract void onJoin(JoinGroupInfo jgi);

    @Override
    public void onRejoin(JoinGroupInfo jgi) {
      log.debug("onRejoin {}", MarlinCoordinator.this.memberId());
      lock.lock();
      rejoinEvent = true;
      condition.signal();
      lock.unlock();
    }

  }
}
