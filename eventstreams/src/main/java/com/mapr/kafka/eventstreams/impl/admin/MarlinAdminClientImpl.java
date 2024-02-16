/* Copyright (c) 2018 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.admin;

import com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils;
import com.mapr.fs.ShimLoader;
import com.mapr.fs.cldb.proto.CLDBProto;
import com.mapr.fs.proto.Common;
import com.mapr.fs.proto.Marlinserver.TopicFeedStatInfo;
import com.mapr.fs.proto.License.LicenseCredentialsMsg;
import com.mapr.fs.proto.License.LicenseIdRequest;
import com.mapr.fs.proto.License.LicenseIdResponse;
import com.mapr.fs.proto.Security.CredentialsMsg;
import com.mapr.db.exceptions.TableNotFoundException;
import com.mapr.kafka.eventstreams.Admin;
import com.mapr.security.UnixUserGroupHelper;
import com.mapr.kafka.eventstreams.TopicDescriptor;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarlinAdminClientImpl extends AdminClient {

  private static final KafkaException emptyTopicNameException = new InvalidTopicException("Topic name \"\" is illegal");

  private static final KafkaException noStreamNameSpecifiedException = new KafkaException("No stream name specified in the topic path or " +
      "in the default stream configuration options");

  static {
    ShimLoader.load();
  }

  private static final Logger LOG = LoggerFactory.getLogger(MarlinAdminClientImpl.class);

  private Admin admin;
  private String defaultStreamName;

  static AdminClient createInternal(AdminClientConfig conf) {
    return new MarlinAdminClientImpl(conf);
  }

  MarlinAdminClientImpl(AdminClientConfig conf) {
    try {
      admin = new MarlinAdminImpl(new Configuration());
      defaultStreamName = conf.getString(AdminClientConfig.STREAMS_ADMIN_DEFAULT_STREAM_CONFIG);
      if (defaultStreamName.equals("")) {
        defaultStreamName = null;
      }
    } catch (Exception e) {
      throw new KafkaException("Failed to create MarlinAdminClient", e);
    }
  }

  // visible for testing
  MarlinAdminClientImpl(Admin admin, String defaultStreamName) {
    this.admin = admin;
    this.defaultStreamName = defaultStreamName;
  }

  public Admin getMapRAdmin() {
    return admin;
  }
  @Override
  public void close(Duration timeout) {
    admin.close(timeout);
  }

  @Override
  public CreateTopicsResult createTopics(Collection<NewTopic> newTopics,
                                         CreateTopicsOptions options) {
    Map<String, KafkaFuture<TopicMetadataAndConfig>> createTopicResult = new HashMap<>();
    String fullTopicPath = null;
    String streamPath = null;
    String topicName = null;

    for (NewTopic n : newTopics) {
      fullTopicPath = n.name();
      String[] tokens = fullTopicPath.split(":");
      KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();

      if (tokens.length == 2) {
        streamPath = tokens[0];
        topicName = tokens[1];
      } else {
        if (tokens[0].startsWith("/")) {
          future.completeExceptionally(
              MarlinAdminClientImpl.emptyTopicNameException);
          createTopicResult.put(fullTopicPath, future);
          continue;
        }
        if (defaultStreamName == null) {
          future.completeExceptionally(
              MarlinAdminClientImpl.noStreamNameSpecifiedException);
          createTopicResult.put(fullTopicPath, future);
          continue;
        }
        streamPath = defaultStreamName;
        topicName = fullTopicPath;
      }

      try {
        admin.createTopic(streamPath, topicName, n.numPartitions());
        future.complete(null);
      } catch (TableNotFoundException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException("Stream " + streamPath +
                                                                          " does not exist."));
      } catch (FileAlreadyExistsException e) {
        future.completeExceptionally(new TopicExistsException(e.getMessage()));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      createTopicResult.put(fullTopicPath, future);
    }

    return new CreateTopicsResult(createTopicResult);
  }

  // Copied from Apache KafkaAdminClient
  @Override
  public DeleteTopicsResult deleteTopics(final TopicCollection topics,
                                         final DeleteTopicsOptions options) {
    if (topics instanceof TopicCollection.TopicIdCollection)
      return DeleteTopicsResult.ofTopicIds(handleDeleteTopicsUsingIds(((TopicCollection.TopicIdCollection) topics).topicIds(), options));
    else if (topics instanceof TopicCollection.TopicNameCollection)
      return DeleteTopicsResult.ofTopicNames(handleDeleteTopicsUsingNames(((TopicCollection.TopicNameCollection) topics).topicNames(), options));
    else
      throw new IllegalArgumentException("The TopicCollection: " + topics + " provided did not match any supported classes for deleteTopics.");
  }

  private Map<String, KafkaFuture<Void>> handleDeleteTopicsUsingNames(final Collection<String> topicNames,
                                                                      final DeleteTopicsOptions options) {
    Map<String, KafkaFuture<Void>> deleteTopicsResult = new HashMap<>();
    String streamPath = null;
    String topicName = null;

    for (String fullTopicPath : topicNames) {
      String[] tokens = fullTopicPath.split(":");
      KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

      if (tokens.length == 2) {
        streamPath = tokens[0];
        topicName = tokens[1];
      } else {
        if (tokens[0].startsWith("/")) {
          future.completeExceptionally(
              MarlinAdminClientImpl.emptyTopicNameException);
          deleteTopicsResult.put(fullTopicPath, future);
          continue;
        }
        if (defaultStreamName == null) {
          future.completeExceptionally(
              MarlinAdminClientImpl.noStreamNameSpecifiedException);
          deleteTopicsResult.put(fullTopicPath, future);
          continue;
        }
        streamPath = defaultStreamName;
        topicName = fullTopicPath;
      }

      try {
        admin.deleteTopic(streamPath, topicName);
        future.complete(null);
      } catch (TableNotFoundException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException("Stream " + streamPath +
                                                                          " does not exist."));
      } catch (NoSuchFileException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException(e.getMessage()));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      deleteTopicsResult.put(fullTopicPath, future);
    }

    return deleteTopicsResult;
  }

  private Map<Uuid, KafkaFuture<Void>> handleDeleteTopicsUsingIds(final Collection<Uuid> topicIds,
                                                                  final DeleteTopicsOptions options) {
    // TODO Marlin
    throw new KafkaException("Topic IDs are not currently supported in MapR Admin. Please use topic names instead");
  }

  @Override
  public ListTopicsResult listTopics(ListTopicsOptions options) {
    if (defaultStreamName == null) {
      throw new KafkaException("No default stream name specified in the configuration options");
    }

    return listTopicsForStream(defaultStreamName, options);
  }

  @Override
  public ListTopicsResult listTopics(String streamPath, ListTopicsOptions options) {
    return listTopicsForStream(streamPath, options);
  }

  private ListTopicsResult listTopicsForStream(String streamPath, ListTopicsOptions options) {
    final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();

    try {
      List<String> topicNamesList = admin.listTopics(streamPath);
      Map<String, TopicListing> topicListingMap = new HashMap<>();

      for (String topicName : topicNamesList) {
        topicListingMap.put(topicName, new TopicListing(topicName, false /*isInternal*/));
      }

      topicListingFuture.complete(topicListingMap);
    } catch (TableNotFoundException e) {
      topicListingFuture.completeExceptionally(new UnknownTopicOrPartitionException("Stream " + streamPath +
                                                                                    " does not exist."));
    } catch (Exception e) {
      topicListingFuture.completeExceptionally(e);
    }
    return new ListTopicsResult(topicListingFuture);
  }

  // Copied from Apache KafkaAdminClient
  @Override
  public DescribeTopicsResult describeTopics(final TopicCollection topics, DescribeTopicsOptions options) {
    if (topics instanceof TopicCollection.TopicIdCollection)
      return DescribeTopicsResult.ofTopicIds(handleDescribeTopicsByIds(((TopicCollection.TopicIdCollection) topics).topicIds(), options));
    else if (topics instanceof TopicCollection.TopicNameCollection)
      return DescribeTopicsResult.ofTopicNames(handleDescribeTopicsByNames(((TopicCollection.TopicNameCollection) topics).topicNames(), options));
    else
      throw new IllegalArgumentException("The TopicCollection: " + topics + " provided did not match any supported classes for describeTopics.");
  }

  private Map<String, KafkaFuture<TopicDescription>> handleDescribeTopicsByNames(final Collection<String> topicNames, DescribeTopicsOptions options) {
    Map<String, KafkaFuture<TopicDescription>> describeTopicsResult = new HashMap<>();
    String streamPath = null;
    String topicName = null;

    for (String fullTopicPath : topicNames) {
      String[] tokens = fullTopicPath.split(":");
      KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();

      if (tokens.length == 2) {
        streamPath = tokens[0];
        topicName = tokens[1];
      } else {
        if (tokens[0].startsWith("/")) {
          future.completeExceptionally(
              MarlinAdminClientImpl.emptyTopicNameException);
          describeTopicsResult.put(fullTopicPath, future);
          continue;
        }
        if (defaultStreamName == null) {
          future.completeExceptionally(
              MarlinAdminClientImpl.noStreamNameSpecifiedException);
          describeTopicsResult.put(fullTopicPath, future);
          continue;
        }
        streamPath = defaultStreamName;
        topicName = fullTopicPath;
      }

      try {
        TopicDescriptor marlinTopDesc = admin.getTopicDescriptor(streamPath, topicName);
        List<TopicPartitionInfo> partitions = new ArrayList<TopicPartitionInfo>();
        Node leader = new Node(0 /*id*/, "127.0.0.1" /*host*/, 7200 /*port*/);
        List<Node> replicas = new ArrayList<Node>();
        replicas.add(leader);

        for (int p = 0; p < marlinTopDesc.getPartitions(); p++) {
          partitions.add(new TopicPartitionInfo(p, leader, replicas, replicas /*isr*/));
        }

        TopicDescription kafkaTopDesc = new TopicDescription(streamPath + ":" + topicName, false /*isInternal*/, partitions);
        future.complete(kafkaTopDesc);
      } catch (TableNotFoundException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException("Stream " + streamPath +
                                                                          " does not exist."));
      } catch (NoSuchFileException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException(e.getMessage()));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      describeTopicsResult.put(streamPath + ":" + topicName, future);
    }

    return describeTopicsResult;
  }

  private Map<Uuid, KafkaFuture<TopicDescription>> handleDescribeTopicsByIds(Collection<Uuid> topicIds, DescribeTopicsOptions options) {
    // TODO Marlin
    throw new KafkaException("Topic IDs are not currently supported in MapR Admin. Please use topic names instead");
  }

  @Override
  public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                                 CreatePartitionsOptions options) {
    Map<String, KafkaFuture<Void>> createPartitionsResult = new HashMap<>();
    String streamPath = null;
    String topicName = null;

    for (String fullTopicPath : newPartitions.keySet()) {
      String[] tokens = fullTopicPath.split(":");
      KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

      if (tokens.length == 2) {
        streamPath = tokens[0];
        topicName = tokens[1];
      } else {
        if (tokens[0].startsWith("/")) {
          future.completeExceptionally(
              MarlinAdminClientImpl.emptyTopicNameException);
          createPartitionsResult.put(fullTopicPath, future);
          continue;
        }
        if (defaultStreamName == null) {
          future.completeExceptionally(
              MarlinAdminClientImpl.noStreamNameSpecifiedException);
          createPartitionsResult.put(fullTopicPath, future);
          continue;
        }
        streamPath = defaultStreamName;
        topicName = fullTopicPath;
      }

      NewPartitions n = newPartitions.get(fullTopicPath);

      try {
        admin.editTopic(streamPath, topicName, n.totalCount());
        future.complete(null);
      } catch (TableNotFoundException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException("Stream " + streamPath +
                                                                          " does not exist."));
      } catch (NoSuchFileException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException(e.getMessage()));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      createPartitionsResult.put(fullTopicPath, future);
    }

    return new CreatePartitionsResult(createPartitionsResult);
  }

  @Override
  public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
                                                             DescribeReplicaLogDirsOptions options) {
    throw new KafkaException("describeReplicaLogDirs API not implemented");
  }

  @Override
  public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers,
                                               DescribeLogDirsOptions options) {
    throw new KafkaException("describeLogDirs API not implemented");
  }

  @Override
  public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs,
                                         AlterConfigsOptions options) {
    throw new KafkaException("alterConfigs API not implemented");
  }

  @Override
  public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource,
          Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
    throw new KafkaException("incrementalAlterConfigs API not implemented");
  }

  @Override
  public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
                                                       AlterReplicaLogDirsOptions options) {
    throw new KafkaException("alterReplicaLogDirs API not implemented");
  }

  @Override
  public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources,
                                               DescribeConfigsOptions options) {
    throw new KafkaException("describeConfigs API not implemented");
  }

  @Override
  public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters,
                                     DeleteAclsOptions options) {
    throw new KafkaException("deleteAcls API not implemented");
  }

  @Override
  public CreateAclsResult createAcls(Collection<AclBinding> acls,
                                     CreateAclsOptions options) {
    throw new KafkaException("createAcls API not implemented");
  }

  @Override
  public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                           DeleteRecordsOptions options) {
    throw new KafkaException("deleteRecords API not implemented");
  }

  @Override
  public DescribeAclsResult describeAcls(AclBindingFilter filter,
                                         DescribeAclsOptions options) {
    throw new KafkaException("describeAcls API not implemented");
  }

  private LicenseIdResponse fetchClusterID (String cluster, CredentialsMsg creds)
      throws Exception {
    byte[] data;
    LicenseIdResponse resp = null;
    LicenseIdRequest req = LicenseIdRequest.newBuilder()
                                           .setCreds(LicenseCredentialsMsg.newBuilder()
                                                                          .setUid(creds.getUid())
                                                                          .addAllGids(creds.getGidsList())
                                                                          .build())
                                           .build();

    try {
      if (cluster != null && !cluster.isEmpty()) {
        data = CLDBRpcCommonUtils.getInstance().sendRequest (
                    cluster,
                    Common.MapRProgramId.CldbProgramId.getNumber(),
                    CLDBProto.CLDBProg.GetLicenseIdProc.getNumber(),
                    req,
                    LicenseIdResponse.class
              );
      } else {
        data = CLDBRpcCommonUtils.getInstance().sendRequest(
                    Common.MapRProgramId.CldbProgramId.getNumber(),
                    CLDBProto.CLDBProg.GetLicenseIdProc.getNumber(),
                    req,
                    LicenseIdResponse.class
          );
      }

      if (data == null) {
        return null;
      }

      // success
      resp = LicenseIdResponse.parseFrom(data);
    } catch (Exception e) {
      throw e;
    }
    return resp;
  }

  private CredentialsMsg getUserCredentials() {
    UnixUserGroupHelper ui = new UnixUserGroupHelper();
    String user = ui.getLoggedinUsername();
    int uid = ui.getUserId(user);

    int [] gids = ui.getGroups(user);

    CredentialsMsg.Builder msg = CredentialsMsg.newBuilder().setUid(uid);
    for (int gid: gids) {
      msg.addGids(gid);
    }
    return msg.build();
  }

  @Override
  public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
    final KafkaFutureImpl<Collection<Node>> describeClusterFuture = new KafkaFutureImpl<>();
    final KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
    final KafkaFutureImpl<String> clusterIdFuture = new KafkaFutureImpl<>();

    String curCluster = CLDBRpcCommonUtils.getInstance().getCurrentClusterName();

    try {
      LicenseIdResponse licId = fetchClusterID(curCluster, getUserCredentials());
      clusterIdFuture.complete(licId.getClusterid());
    } catch (Exception e) {
      clusterIdFuture.completeExceptionally(e);
    }

    Node controller = new Node(0 /*id*/, "127.0.0.1" /*host*/, 7200 /*port*/);
    List<Node> brokers = new ArrayList<Node>();
    brokers.add(controller);
    describeClusterFuture.complete(brokers);
    controllerFuture.complete(controller);

    return new DescribeClusterResult((KafkaFuture<Collection<Node>>) describeClusterFuture,
                                     (KafkaFuture<Node>) controllerFuture,
                                     (KafkaFuture<String>) clusterIdFuture,
                                     null);
  }

  @Override
  public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
    throw new KafkaException("describeConsumerGroups API not implemented");
  }

  @Override
  public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
    if (defaultStreamName == null) {
      throw new KafkaException("No default stream name specified in the configuration options");
    }

    final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl<>();

    try {
      all.complete(admin.listConsumerGroups(defaultStreamName));
    } catch (IOException e) {
      all.completeExceptionally(e);
    }

    return new ListConsumerGroupsResult(all);
  }

  @Override
  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
                                                                 ListConsumerGroupOffsetsOptions options) {
    if (defaultStreamName == null) {
      throw new KafkaException("No default stream name specified in the configuration options");
    }

    return listConsumerGroupOffsets(defaultStreamName, groupSpecs);
  }

  // used by kwp
 public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String stream, String groupId) {
    return listConsumerGroupOffsets(stream, Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec()));
  }

  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String stream, Map<String, ListConsumerGroupOffsetsSpec> groupSpecs) {

    if (groupSpecs.size() != 1) {
      // TODO Marlin: implement handling groupSpecs. So far handling only one group
      throw new KafkaException("listConsumerGroupOffsets with more than 1 group is not currently supported in MapR Admin");
    }
    String groupId = groupSpecs.keySet().stream().findAny().get();
    final KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> groupOffsetListingFuture = new KafkaFutureImpl<>();

    try {
      groupOffsetListingFuture.complete(admin.listConsumerGroupOffsets(stream, groupId));
    } catch (IOException e) {
      groupOffsetListingFuture.completeExceptionally(e);
    }

    return new ListConsumerGroupOffsetsResult(
            Collections.singletonMap(CoordinatorKey.byGroupId(groupId), groupOffsetListingFuture));
  }

  @Override
  public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
    throw new KafkaException("deleteConsumerGroups API not implemented");
  }

  @Override
  public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId,
                                                                     Set<TopicPartition> partitions,
                                                                     DeleteConsumerGroupOffsetsOptions options) {
    throw new KafkaException("deleteConsumerGroupOffsets API not implemented");
  }

  @Override
  public ElectLeadersResult electLeaders(ElectionType electionType,
                                         Set<TopicPartition> partitions,
                                         ElectLeadersOptions options) {
    throw new KafkaException("electLeaders API not implemented");
  }

  @Override
  public AlterPartitionReassignmentsResult alterPartitionReassignments(
          Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
          AlterPartitionReassignmentsOptions options) {
    throw new KafkaException("alterPartitionReassignments API not implemented");
  }

  @Override
  public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                      ListPartitionReassignmentsOptions options) {
    throw new KafkaException("listPartitionReassignments API not implemented");
  }

  @Override
  public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
                                                                             RemoveMembersFromConsumerGroupOptions options) {
    throw new KafkaException("removeMembersFromConsumerGroup API not implemented");
  }

  @Override
  public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId,
                                                                   Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                   AlterConsumerGroupOffsetsOptions options) {
    final KafkaFutureImpl<Map<TopicPartition, Errors>> groupOffsetAlterFutures = new KafkaFutureImpl<>();

    try {
      groupOffsetAlterFutures.complete(
          admin.alterConsumerGroupOffsets(defaultStreamName, groupId, offsets));
    } catch (IOException e) {
      groupOffsetAlterFutures.completeExceptionally(e);
    }

    return new AlterConsumerGroupOffsetsResult(groupOffsetAlterFutures);
  }

  @Override
  public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                       ListOffsetsOptions options) {
    final Map<TopicPartition, KafkaFutureImpl<ListOffsetsResultInfo>> futures = new HashMap<>(topicPartitionOffsets.size());
    final Map<String, Map<String, List<TopicFeedInfo>>> streamsInfo = new HashMap<>(topicPartitionOffsets.size());

    for (TopicPartition topicPartition : topicPartitionOffsets.keySet()) {
      String fullTopicPath = topicPartition.topic();

      String streamPath = null;
      String topicName = null;

      String[] tokens = fullTopicPath.split(":");
      KafkaFutureImpl<ListOffsetsResultInfo> future = new KafkaFutureImpl<>();

      if (tokens.length == 2) {
        streamPath = tokens[0];
        topicName = tokens[1];
      } else {
        if (tokens[0].startsWith("/")) {
          future.completeExceptionally(
              MarlinAdminClientImpl.emptyTopicNameException);
          futures.put(topicPartition, future);
          continue;
        }
        if (defaultStreamName == null) {
          future.completeExceptionally(
              MarlinAdminClientImpl.noStreamNameSpecifiedException);
          futures.put(topicPartition, future);
          continue;
        }
        streamPath = defaultStreamName;
        topicName = fullTopicPath;
      }

      try {
        Map<String, List<TopicFeedInfo>> topicsForStream;
        if (streamsInfo.containsKey(streamPath)) {
          topicsForStream = streamsInfo.get(streamPath);
        } else {
          topicsForStream = admin.listTopicsForStream(streamPath);
          streamsInfo.put(streamPath, topicsForStream);
        }

        if (topicsForStream.containsKey(topicName)) {
          boolean checkPartitionExist = false;

          for (TopicFeedInfo topicFeedInfo : topicsForStream.get(topicName)) {
            TopicFeedStatInfo tinfo = topicFeedInfo.stat();

            if (topicPartition.partition() == tinfo.getFeedId()) {
              future.complete(new ListOffsetsResultInfo(tinfo.getMaxSeq(), tinfo.getTimeRange().getMaxTS(), Optional.empty()));
              checkPartitionExist = true;
              break;
            }
          }

          if (!checkPartitionExist) {
            future.completeExceptionally(new UnknownTopicOrPartitionException(
                    String.format("Partition %d cannot be found in the topic %s",
                            topicPartition.partition(), topicPartition.topic())));
          }
        } else {
          future.completeExceptionally(new UnknownTopicOrPartitionException(
                  String.format("Topic %s cannot be found in the stream %s", topicName, streamPath)));
        }
      } catch (TableNotFoundException e) {
        future.completeExceptionally(new UnknownTopicOrPartitionException("Stream " + streamPath +
                " does not exist."));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      futures.put(topicPartition, future);
    }

    return new ListOffsetsResult(new HashMap<>(futures));
  }

  @Override
  public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter,
                                                         DescribeClientQuotasOptions options) {
    throw new KafkaException("describeClientQuotas API not implemented");
  }

  @Override
  public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries,
                                                   AlterClientQuotasOptions options) {
    throw new KafkaException("alterClientQuotas API not implemented");
  }

  @Override
  public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
    // TODO Malin
    throw new KafkaException("describeUserScramCredentials method is not currently supported in MapR Admin");
  }

  @Override
  public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
    // TODO Malin
    throw new KafkaException("alterUserScramCredentials method is not currently supported in MapR Admin");
  }

  @Override
  public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
    // TODO Malin
    throw new KafkaException("describeFeatures method is not currently supported in MapR Admin");
  }

  @Override
  public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
    // TODO Malin
    throw new KafkaException("updateFeatures method is not currently supported in MapR Admin");
  }

  @Override
  public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
    // TODO Malin
    throw new KafkaException("describeMetadataQuorum method is not currently supported in MapR Admin");
  }

  @Override
  public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
    // TODO Malin
    throw new KafkaException("unregisterBroker method is not currently supported in MapR Admin");
  }

  @Override
  public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options) {
    // TODO Malin
    throw new KafkaException("describeProducers method is not currently supported in MapR Admin");
  }

  @Override
  public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
    // TODO Malin
    throw new KafkaException("describeTransactions method is not currently supported in MapR Admin");
  }

  @Override
  public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
    // TODO Malin
    throw new KafkaException("abortTransaction method is not currently supported in MapR Admin");
  }

  @Override
  public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
    // TODO Malin
    throw new KafkaException("listTransactions method is not currently supported in MapR Admin");
  }

  @Override
  public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
    // TODO Malin
    throw new KafkaException("fenceProducers method is not currently supported in MapR Admin");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new KafkaException("metrics API not implemented");
  }

  @Override
  public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
    throw new KafkaException("createDelegationToken API not implemented");
  }

  @Override
  public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
    throw new KafkaException("renewDelegationToken API not implemented");
  }

  @Override
  public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
    throw new KafkaException("expireDelegationToken API not implemented");
  }

  @Override
  public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
    throw new KafkaException("describeDelegationToken API not implemented");
  }

}
