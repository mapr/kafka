package com.mapr.kafka.eventstreams.impl.listener;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
//import org.omg.stub.java.rmi._Remote_Stub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;

import com.mapr.fs.proto.Marlinserver.JoinGroupDesc;
import com.mapr.fs.proto.Marlinserver.JoinGroupInfo;
import com.mapr.fs.proto.Marlinserver.Member;
import com.mapr.fs.proto.Marlinserver.MemberProtocol;
import com.mapr.fs.proto.Marlinserver.MemberState;
import com.mapr.kafka.eventstreams.TopicRefreshListListener;
import com.mapr.kafka.eventstreams.TopicRefreshRegexListener;
import com.mapr.kafka.eventstreams.impl.MarlinCoordinator;
import com.mapr.kafka.eventstreams.impl.listener.MarlinListener.MarlinJoinCallback;
import java.util.Collections;

public class MarlinConsumerCoordinator extends MarlinCoordinator {
  private final MarlinListener<?, ?> listener;
  private final MarlinListenerImpl listenerimpl;
  private final Thread pollThread = new Thread(new ConsumerPollThread());
  private final SubscriptionState subscriptionState = new SubscriptionState();
  private ConsumerRebalanceListener rebalanceCb;
  private final ConsumerPartitionAssignor assignor;
  private final AtomicBoolean closing = new AtomicBoolean(false);
  private final TopicRefreshCCRegexListener regexRefreshListener = new TopicRefreshCCRegexListener();
  private final TopicRefreshCCListListener listRefreshListener = new TopicRefreshCCListListener();
  private boolean isLeader = false;
  private JoinGroupInfo leaderJGI;
  private boolean leaderGroupJoinInProgress = false;
  private Cluster clusterWithTopicInfo;
  private boolean rejoinInProgress = false;
  private ConsumerGroupMetadata groupMetadata;

  // For MEP 5.0, we will use /var/mapr/kafka-internal-stream.
  //   See MS-55 for default internal stream support for GA.
  private String internalStream;

  public MarlinConsumerCoordinator(MarlinListener<?, ?> listener, MarlinListenerImpl listenerimpl, String groupId,
      List<ConsumerPartitionAssignor> assignors, String intStream, ConsumerGroupMetadata groupMetadata) {
    super(groupId);
    internalStream = intStream;
    this.listener = listener;
    this.listenerimpl = listenerimpl;
    // TODO Support multiple assignors once multiple protocol support is added to group-join protocol.
    this.assignor = assignors.get(0);
    this.groupMetadata = groupMetadata;
    pollThread.start();
    init();
    log.debug("MarlinConsumerCoordinator constructor");
  }

  private enum SubscriptionType {
    NONE, SUBSCRIBE_LIST, SUBSCRIBE_REGEX, ASSIGN_PARTITIONS
  }

  private class SubscriptionState {
    private static final String EXCEPTION_MESSAGE = "Subscription to topics, partitions and pattern are mutually exclusive";

    /* Subscription Type */
    private SubscriptionType subscriptionType = SubscriptionType.NONE;

    /* Subscribed Regex pattern */
    private Pattern subscribedRegex;

    /*
     * The list of subscribed topics. If subscription-type is Regex, this list
     * contains list of subscriptions that match regex.
     */
    private Set<String> subscriptions;

    /*
     * The list of topics the group has subscribed to. This is only tracked by
     * the leader.
     */
    private final Set<String> groupSubscriptions;

    /*
     * The partitions that are currently assigned.
     */
    private final Set<TopicPartition> assignments;

    public SubscriptionState() {
      this.groupSubscriptions = new HashSet<String>();
      this.assignments = new HashSet<TopicPartition>();
      this.subscriptions = new HashSet<String>();
    }

    private synchronized void setSubscriptionType(SubscriptionType type) {
      if (this.subscriptionType == SubscriptionType.NONE)
        this.subscriptionType = type;
      else if (this.subscriptionType != type)
        throw new IllegalStateException(EXCEPTION_MESSAGE);
    }

    private synchronized SubscriptionType getSubscriptionType() {
      return this.subscriptionType;
    }

    private synchronized void subscribe(Pattern pattern, SubscriptionType type) {
      this.setSubscriptionType(type);
      this.subscribedRegex = pattern;
    }

    private synchronized void subscribe(Collection<String> topics, SubscriptionType type) {
      assert (this.subscriptions != null);
      assert (this.subscriptions.size() == 0);

      this.setSubscriptionType(type);
      for (String topic : topics) {
        this.subscriptions.add(topic);
      }
    }

    private synchronized void assign(Collection<TopicPartition> assignments, SubscriptionType type) {
      if (type == SubscriptionType.ASSIGN_PARTITIONS) {
        clearAssignments();
      }
      assert (this.assignments != null);
      assert (this.assignments.size() == 0);

      this.setSubscriptionType(type);
      for (TopicPartition topicPart : assignments) {
        this.assignments.add(topicPart);
      }
    }

    private synchronized void unsubscribe() {
      this.subscriptions.clear();
      this.subscribedRegex = null;
      this.subscriptionType = SubscriptionType.NONE;
    }

    private synchronized Set<TopicPartition> assignment() {
      return Collections.unmodifiableSet(new HashSet<>(this.assignments));
    }

    private synchronized List<TopicPartition> assignmentList() {
      return Collections.unmodifiableList(new ArrayList<>(this.assignments));
    }

    private synchronized Set<String> subscription() {
      return Collections.unmodifiableSet(new HashSet<>(this.subscriptions));
    }

    private synchronized void clearAssignments() {
      this.assignments.clear();
    }

    private synchronized void updateSubscriptions(Collection<String> topics) {
      this.subscriptions.clear();
      for (String topic : topics) {
        this.subscriptions.add(topic);
      }
    }

    private synchronized Pattern getSubscribedRegex() {
      return subscribedRegex;
    }

    private synchronized void clearGroupSubscriptions() {
      groupSubscriptions.clear();
    }

    private synchronized void setGroupSubscriptions(Collection<String> allSubscribedTopics) {
      groupSubscriptions.clear();
      groupSubscriptions.addAll(allSubscribedTopics);
    }
  }

  public class TopicRefreshCCListListener implements TopicRefreshListListener {
    @Override
    public void updatedTopics(Set<TopicPartition> topicFeeds) {
      log.debug("ListRefresh updated topic info {} ", topicFeeds);
      handleTopicRefresherList(topicFeeds);
    }
  }

  public class TopicRefreshCCRegexListener implements TopicRefreshRegexListener {
    @Override
    public void updatedTopics(Set<String> topics) {
      log.debug("RegexRefresh updated topic info {} ", topics);
      handleTopicRefresherRegex(topics);
    }
  }

  private final class ConsumerPollThread implements Runnable {

    @Override
    public void run() {
      while (true) {

        pollEvent(Long.MAX_VALUE);
        if (closing.get() == true) {
          return;
        }

        // Optimization, if this is a group rejoin event, refresh the regex and join to avoid needless rejoins later.
        if (rejoinEventOccured() && subscriptionState.getSubscriptionType() == SubscriptionType.SUBSCRIBE_REGEX) {
          resetRejoinFlags();
          log.debug("Rejoin event occured, refreshing regex with pattern {} ", subscriptionState.getSubscribedRegex());
          listenerimpl.topicRefresherRegex(subscriptionState.getSubscribedRegex(), regexRefreshListener);
        }

        ensureActiveGroup();
      }
    }
  }

  public ConsumerGroupMetadata groupMetadata() {
    return groupMetadata;
  }

  @Override
  public void requestRejoin() {
    synchronized (this) {
      rejoinInProgress = true;
    }
    super.requestRejoin();
  }

  private void handleTopicRefresherRegex(Set<String> topics) {
    subscriptionState.updateSubscriptions(topics);
    requestRejoin();
  }

  private Cluster createClusterWithTopicMeta(Set<TopicPartition> topicsWithMaxFeeds) {
    Set<PartitionInfo> partInfo = new HashSet<>();
    for (TopicPartition topicPart : topicsWithMaxFeeds) {
      // topicPart.partition() represent the numFeeds in this topic.
      for (int i = 0; i < topicPart.partition(); i++) {
        PartitionInfo p = new PartitionInfo(topicPart.topic(), i, null, null, null, null);
        partInfo.add(p);
      }
    }
    Cluster clusterWithTopicInfo = new Cluster("mapR", Collections.emptyList(), partInfo, Collections.emptySet(),
        Collections.emptySet());
    return clusterWithTopicInfo;
  }

  private void handleTopicRefresherList(Set<TopicPartition> topicsWithMaxFeeds) {
    assert (isLeader == true);
    if (leaderGroupJoinInProgress) {
      log.debug("Leader join. groupSubscription topic info: {} ", topicsWithMaxFeeds);
      assert (leaderJGI != null);
      assert (clusterWithTopicInfo == null);
      clusterWithTopicInfo = createClusterWithTopicMeta(topicsWithMaxFeeds);
      performOnJoin(leaderJGI);
      leaderGroupJoinInProgress = false;
      leaderJGI = null;
      clusterWithTopicInfo = null;
    } else {
      // Request rejoin if the numPartitions changed for any of the
      // groupSubscriptions.
      requestRejoin();
    }
  }

  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    log.debug("Subscribe with regex, begin : {} ", pattern.toString());
    waitForRejoinCompletion();
    subscriptionState.subscribe(pattern, SubscriptionType.SUBSCRIBE_REGEX);
    rebalanceCb = callback;
    synchronized (this) {
      rejoinInProgress = true;
    }
    listenerimpl.topicRefresherRegex(subscriptionState.getSubscribedRegex(), regexRefreshListener);
    waitForRejoinCompletion();
    log.debug("Subscribe with regex, end: {}", pattern.toString());
  }

  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) throws KafkaException {
    log.debug("Subscribe with topics, begin : {} ", topics);
    waitForRejoinCompletion();
    subscriptionState.subscribe(topics, SubscriptionType.SUBSCRIBE_LIST);
    rebalanceCb = callback;
    // Trigger a rejoin with new subscriptions.
    requestRejoinAndWait();
    log.debug("Subscribe with topics, end: {}", topics);
  }

  public void assign(Collection<TopicPartition> partitions) {
    log.debug("Assign with topic partitions, begin : {} ", partitions);
    waitForRejoinCompletion();
    subscriptionState.assign(partitions, SubscriptionType.ASSIGN_PARTITIONS);
    listenerimpl.assign(partitions);
    log.debug("Assign with topic partitions, end: {} ", partitions);
  }

  public void unsubscribe() {
    log.debug("Unsubscribe, begin");
    if (subscriptionState.getSubscriptionType() == SubscriptionType.NONE) {
      return;
    }
    waitForRejoinCompletion();
    if (subscriptionState.getSubscriptionType() == SubscriptionType.SUBSCRIBE_REGEX) {
      listenerimpl.topicRefresherRegex(null, null);
    }
    if (isLeader) {
      listenerimpl.topicRefresherList(Collections.emptySet(), null);
    }
    subscriptionState.unsubscribe();
    // MS-123 We don't have graceful group leave semantics.
    //  During close, we stop the heart beat and eventually gets kicked out of the group.
    //  TODO (MS-163) Implement group leave semantics and use it instead.
    if (!closing.get()) {
      requestRejoinAndWait();
    }
    listenerimpl.unsubscribe();
    log.debug("Unsubscribe, end");
  }

  private synchronized void notifyRejoinCompletion() {
    rejoinInProgress = false;
    this.notifyAll();
  }

  private synchronized void requestRejoinAndWait() {
    requestRejoin();
    waitForRejoinCompletion();
  }

  private synchronized void waitForRejoinCompletion() {
    while (rejoinInProgress) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
        log.debug("Unsubscribe: interrupted");
      }
    }
  }

  public Set<TopicPartition> assignment() {
    return subscriptionState.assignment();
  }

  public Set<String> subscription() {
    return subscriptionState.subscription();
  }

  @Override
  protected Logger getLogger() {
    return LoggerFactory.getLogger(MarlinConsumerCoordinator.class);
  }

  @Override
  protected MarlinJoinCallback getJoinerCallback() {
    return new MarlinConsumerJoinCallback();
  }

  @Override
  protected String generateSyncTopic(String groupId) {
    String topic = "__mapr__" + groupId + "_assignment";
    return topic;
  }

  @Override
  protected String generateCoordStream() {
    return internalStream;
  }

  @Override
  protected JoinGroupDesc generateJoinDesc() {
    Set<String> joinedSubscription = subscriptionState.subscription();
    List<String> topics = new ArrayList<>(joinedSubscription);
    Subscription subscription = new Subscription(topics,
                                                 assignor.subscriptionUserData(joinedSubscription),
                                                 subscriptionState.assignmentList());
    ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

    JoinGroupDesc desc = JoinGroupDesc.newBuilder().setProtocolType("consumer").setMemberId(this.memberId)
        .addMemberProtocols(
            MemberProtocol.newBuilder().setProtocol(assignor.name()).setMemberMetadata(ByteString.copyFrom(metadata))
                .build())
        .build();
    return desc;
  }

  @Override
  protected void revokeAssignments() {
    Collection<TopicPartition> revoked = subscriptionState.assignment();
    log.debug("Revoking partition assignments {}", revoked);
    try {
      if (rebalanceCb != null) {
        rebalanceCb.onPartitionsRevoked(revoked);
      }
    } catch (Exception e) {
      log.error("User provided listener {} failed on partition revocation", rebalanceCb.getClass().getName(), e);
    }
    subscriptionState.clearAssignments();
    listenerimpl.unsubscribe();
  }

  @Override
  protected void protocolOnSyncComplete(MemberState ms, long generationId) {
    assert (assignor != null);

    Assignment assignment = ConsumerProtocol.deserializeAssignment(ms.getMemberAssignment().asReadOnlyByteBuffer());

    Collection<TopicPartition> assignments = assignment.partitions();

    if (subscriptionState.getSubscriptionType() == SubscriptionType.NONE) {
      notifyRejoinCompletion();
      return;
    }

    assert (subscriptionState.getSubscriptionType() == SubscriptionType.SUBSCRIBE_LIST
        || subscriptionState.getSubscriptionType() == SubscriptionType.SUBSCRIBE_REGEX);

    if (subscriptionState.getSubscriptionType() == SubscriptionType.SUBSCRIBE_REGEX) {
      String[] tokens = subscriptionState.getSubscribedRegex().toString().split(":");
      // Get the token part to make sure /Stream:^topic* matches with
      // /Stream:topic5
      Pattern topicRegex = Pattern.compile(tokens[tokens.length - 1]);
      for (TopicPartition tp : assignments) {
        String[] topicTokens = tp.topic().split(":");
        if (!topicRegex.matcher(topicTokens[topicTokens.length - 1]).matches())
          throw new IllegalArgumentException(
              "Assigned partition " + tp + " for non-subscribed topic regex pattern; Subscription regex is "
                  + subscriptionState.getSubscribedRegex());
      }
    } else {
      Set<String> subscriptions = subscriptionState.subscription();
      for (TopicPartition tp : assignments)
        if (!subscriptions.contains(tp.topic()))
          throw new IllegalArgumentException(
              "Assigned partition " + tp + " for non-subscribed topic; subscription is " + subscriptionState);
    }

    subscriptionState.assign(assignments, subscriptionState.getSubscriptionType());

    // Call assignor onAssignment callback.
    assignor.onAssignment(assignment, groupMetadata);
    log.info("Setting newly assigned partitions {}", subscriptionState.assignments);
    try {
      if (rebalanceCb != null) {
        rebalanceCb.onPartitionsAssigned(subscriptionState.assignment());
      }
    } catch (Exception e) {
      log.error("User provided listener {} failed on partition assignment", rebalanceCb.getClass().getName(), e);
    }

    // Now call the assign to jni layer to setup message fetcher and
    // auto-commit.
    listenerimpl.assign(assignments);
    notifyRejoinCompletion();
  }

  @Override
  protected boolean isProtocolRejoinNeeded() {
    return false;
  }

  @Override
  public void close() {
    log.debug("close , begin");
    closing.set(true);
    unsubscribe();
    wakeup();

    try {
      pollThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    super.close();
    log.debug("close , end");
  }

  public class MarlinConsumerJoinCallback extends MarlinCoordinatorJoinCallback {

    @Override
    public void onJoin(JoinGroupInfo jgi) {

      // OnJoin in called in heart beat context. assignor.assign or
      // TopicRefreshList can take time. Issue TopicRefreshList and
      // perform onJoin in the callback context.
      assert (leaderGroupJoinInProgress == false);
      assert (leaderJGI == null);
      if (jgi.getGroupLeaderId().equals(MarlinConsumerCoordinator.this.memberId())) {
        log.debug(" Group Leader ");

        leaderGroupJoinInProgress = true;
        leaderJGI = jgi;
        isLeader = true;

        // Get the max partitions for all the groups subscribed topics.
        // Continue the onJoin in TopicRefresherList callback.
        Collection<String> groupSubscriptions = getAllSubscribedTopics(jgi.getMembersList());
        if (groupSubscriptions.size() > 0) {
          listenerimpl.topicRefresherList(groupSubscriptions, listRefreshListener);
        } else {
          clusterWithTopicInfo = createClusterWithTopicMeta(Collections.emptySet());
          performOnJoin(jgi);
          leaderGroupJoinInProgress = false;
          leaderJGI = null;
          clusterWithTopicInfo = null;
        }
      } else {
        log.debug(" Group Follower ");
        // Clear the group subscriptions if we were the leader before.
        subscriptionState.clearGroupSubscriptions();
        listenerimpl.topicRefresherList(Collections.emptyList(), null);
        isLeader = false;
        performOnJoin(jgi);
      }
    }
  }

  @Override
  protected Map<String, ByteBuffer> performProtocolAssignment(String leaderId, List<Member> members) {
    assert (isLeader == true);
    Map<String, Subscription> subscriptions = new HashMap<>();
    Set<String> allSubscribedTopics = new HashSet<>();
    for (Member member : members) {
      Subscription subscription = ConsumerProtocol
          .deserializeSubscription(member.getMemberMetadata().asReadOnlyByteBuffer());
      subscriptions.put(member.getMemberId(), subscription);
      allSubscribedTopics.addAll(subscription.topics());
    }

    subscriptionState.setGroupSubscriptions(allSubscribedTopics);

    log.debug("Performing assignment using {} with subscriptions {}", assignor.name(), subscriptions);

    assert (clusterWithTopicInfo != null);
    Map<String, Assignment> assignment = assignor
            .assign(clusterWithTopicInfo, new GroupSubscription(subscriptions))
            .groupAssignment();

    Set<String> assignedTopics = new HashSet<>();
    for (Assignment assigned : assignment.values()) {
      for (TopicPartition tp : assigned.partitions())
        assignedTopics.add(tp.topic());
    }

    if (!assignedTopics.containsAll(allSubscribedTopics)) {
      Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
      notAssignedTopics.removeAll(assignedTopics);
      log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
    }

    // Now create the assignment and return.
    Map<String, ByteBuffer> groupAssignment = new HashMap<>();
    for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
      ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
      groupAssignment.put(assignmentEntry.getKey(), buffer);
    }

    return groupAssignment;
  }

  private Collection<String> getAllSubscribedTopics(List<Member> members) {
    Set<String> allSubscribedTopics = new HashSet<>();
    for (Member member : members) {
      Subscription subscription = ConsumerProtocol
          .deserializeSubscription(member.getMemberMetadata().asReadOnlyByteBuffer());
      allSubscribedTopics.addAll(subscription.topics());
    }
    return allSubscribedTopics;
  }
}
