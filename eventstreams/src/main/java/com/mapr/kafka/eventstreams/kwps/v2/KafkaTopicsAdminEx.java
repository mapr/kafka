/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps.v2;

import static com.mapr.kwps.KwpsCommon.KWPS_NEW_TOPIC_OWN_VOLUME;
import static com.mapr.kwps.KwpsCommon.KWPS_TOPICS_FOLDER;
import static com.mapr.kwps.KwpsCommon.KWPS_TOPICS_FOLDER_PATH;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import com.mapr.fs.MapRFileStatus;
import com.mapr.fs.proto.Security.CredentialsMsg;
import com.mapr.kwps.KTopicDescriptor;
import com.mapr.kwps.KwpsCommon;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This class provides additional Admin capabilities for use by Kafka Wire Protocol Service
 */
@Slf4j
public class KafkaTopicsAdminEx extends KafkaTopicsAdminV2 {

  public KafkaTopicsAdminEx() throws IOException {
  }

  public KafkaTopicsAdminEx(@NonNull final String maprUser) throws IOException {
    this(maprUser, null);
  }

  public KafkaTopicsAdminEx(@NonNull final String maprUser, final CredentialsMsg userCredentials) throws IOException {
    super(maprUser, userCredentials);
  }

  public List<String> getAllTopics() {
    final List<String> topicList = new LinkedList<>();
    try {
      MapRFileStatus[] all = null;
      try {
        all = mfs.listStatus(KWPS_TOPICS_FOLDER_PATH);
      } catch (FileNotFoundException e) {
        new KafkaException("Kafka topics folder '" + KWPS_TOPICS_FOLDER + "' doesn't exist on the cluster.", e);
      }

      for (MapRFileStatus fStatus : all) {
        final Path fPath = fStatus.getPath();
        if (fStatus.isDirectory()) {
          final String topicName = fPath.getName();
          topicList.add(topicName);
        } else {
          log.warn("Unknown file '{}' in Kafka topics folder!", fPath);
        }
      }
      return topicList;
   } catch (IOException e) {
      throw new KafkaException(e);
    }
  }

  public List<String> getAllStreams() {
    final List<String> streamList = new LinkedList<>();
    try {
      MapRFileStatus[] all = null;
      try {
        all = mfs.listStatus(KWPS_TOPICS_FOLDER_PATH);
      } catch (FileNotFoundException e) {
        new KafkaException("Kafka topics folder '" + KWPS_TOPICS_FOLDER + "' doesn't exist on the cluster.", e);
      }

      for (MapRFileStatus fStatus : all) {
        final Path fPath = fStatus.getPath();
        if (fStatus.isDirectory()) {
          final String topicName = fPath.getName();
          streamList.add(KwpsCommon.getStreamPath(topicName));
        } else {
          log.warn("Unknown file '{}' in Kafka topics folder!", fPath);
        }
      }
      return streamList;
    } catch (IOException e) {
      throw new KafkaException(e);
    }
  }

  public CreateTopicsResult createTopics(@NonNull List<NewTopic> fullTopicPaths) {
    final Map<String, KafkaFuture<TopicMetadataAndConfig>> createTopicResult = new HashMap<>();
    for (NewTopic nt : fullTopicPaths) {
      final String fullTopicPath = nt.name();
      final KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
      try {
        boolean ownVolume = false;
        final Map<String, String> configs = nt.configs();
        if (configs != null) {
          final String createVolumeProp = configs.get(KWPS_NEW_TOPIC_OWN_VOLUME);
          if (createVolumeProp != null) {
            ownVolume = Boolean.valueOf(createVolumeProp);
          } else {
            // kafka-topics.sh doesn't like any random config name to be passed,
            // so we piggyback on "preallocate"
            final String preallocate = configs.get(TopicConfig.PREALLOCATE_CONFIG);
            ownVolume = Boolean.valueOf(preallocate);
          }
        }

        final String topicName = getTopicName(fullTopicPath);
        final KTopicDescriptor ktopicDesc = new KTopicDescriptor(topicName)
                                            .setPartitions(nt.numPartitions())
                                            .setOwnVolume(ownVolume);
        createTopic(ktopicDesc);
        future.complete(null);
      } catch (Throwable e) {
        future.completeExceptionally(e);
      }
      createTopicResult.put(fullTopicPath, future);
    }

    return new CreateTopicsResult(createTopicResult);
  }

  public DescribeTopicsResult describeTopics(@NonNull List<String> fullTopicPaths) {
    return adminClient.describeTopics(fullTopicPaths);
  }

  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(@NonNull String streamPath, @NonNull String groupId) {
    return adminClient.listConsumerGroupOffsets(streamPath, groupId);
  }

  public DeleteTopicsResult deleteTopics(@NonNull List<String> fullTopicPaths) {
    final Map<String, KafkaFuture<Void>> deleteTopicsResult = new HashMap<>();

    for (final String fullTopicPath : fullTopicPaths) {
      KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
      try {
        final String topicName = getTopicName(fullTopicPath);
        deleteTopic(topicName);
        future.complete(null);
      } catch (Throwable e) {
        future.completeExceptionally(e);
      }
      deleteTopicsResult.put(fullTopicPath, future);
    }

    return new DeleteTopicsResult(deleteTopicsResult);
  }

  private String getTopicName(final String fullTopicPath) {
    final String[] tokens = fullTopicPath.split(":");
    if (tokens.length != 2) {
      throw new IllegalArgumentException(fullTopicPath + " is not a valid Data Fabric topic path");
    }
    return tokens[1];
  }

}
