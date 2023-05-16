/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams;

import com.mapr.kafka.eventstreams.impl.admin.TopicFeedInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;

public interface Admin extends AutoCloseable {
  /**
   * Create a stream.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param desc descriptor for stream attributes
   */
  public void createStream(String streamPath, StreamDescriptor desc)
      throws IOException, IllegalArgumentException;

  /**
   * Modify the attributes of a stream.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param desc descriptor for stream attributes
   */
  public void editStream(String streamPath, StreamDescriptor desc)
      throws IOException, IllegalArgumentException;

  /**
   * Get the StreamDescriptor for a stream.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @return descriptor for stream attributes
   */
  public StreamDescriptor getStreamDescriptor(String streamPath)
      throws IOException, IllegalArgumentException;

  /**
   * Delete a stream.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   */
  public void deleteStream(String streamPath)
      throws IOException, IllegalArgumentException;

  /**
   * Count the number of topics in a stream.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @return number of topics in the stream.
   */
  public int countTopics(String streamPath)
      throws IOException, IllegalArgumentException;

  /**
   * Create a topic with the default number of partitions.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param topicName name of the topic
   */
  public void createTopic(String streamPath, String topicName)
      throws IOException;

  /**
   * Create a topic with the specified number of partitions.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param topicName name of the topic
   */
  public void createTopic(String streamPath, String topicName, int npartitions)
      throws IOException;

  /**
   * Create a topic with the default number of partitions.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param topicName name of the topic
   * @param desc descriptor for topic attributes
   */
  public void createTopic(String streamPath, String topicName, TopicDescriptor desc)
      throws IOException;

  /**
   * Modify the  number of partitions for a topic.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param topicName name of the topic
   */
  public void editTopic(String streamPath, String topicName, int npartitions)
      throws IOException;

  /**
   * Create a topic with the default number of partitions.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param topicName name of the topic
   * @param desc descriptor for topic attributes
   */
  public void editTopic(String streamPath, String topicName, TopicDescriptor desc)
      throws IOException;

  /**
   * Delete a topic.
   *
   * @param streamPath absolute path of the stream in MapR-FS
   * @param topicName name of the topic
   */
  public void deleteTopic(String streamPath, String topicName)
      throws IOException;

  /*
   * API for internal use only.
   * Prioritize the compaction of the specified topic. Compaction must be
   * enabled on the stream prior to invoking this AP
   *
   * @param streamPath
   *          absolute path of the stream in MapR-FS
   * @param topicName
   *          name of the topic
   */
  public void compactTopicNow(String streamPath, String topicName)
      throws IOException;

  /**
   *
   * @param streamPath
   * @param topicName
   * @return descriptor for topic attributes
   * @throws IOException
   */
  public TopicDescriptor getTopicDescriptor(String streamPath, String topicName)
      throws IOException;

  /**
   * List all the topics within a stream.
   *
   * @param streamPath
   * @return list of topic names for the topics in the stream.
   */
  public List<String> listTopics(String streamPath)
      throws IOException, IllegalArgumentException;

  /**
   * List all the topics within a stream.
   *
   * @param streamPath
   * @return list of topic names for the topics in the stream.
   */
  public Map<String, List<TopicFeedInfo>> listTopicsForStream(String streamPath)
          throws IOException, IllegalArgumentException;

  /**
   * List all the consumer groups within a stream.
   *
   * @param streamPath
   * @return list of consumer groups for the stream.
   */
  public Collection<Object> listConsumerGroups(String streamPath)
          throws IOException, IllegalArgumentException;

  /**
   * @param streamPath
   * @return {@code true} if the specified path exists and is a stream
   */
  public boolean streamExists(String streamPath)
      throws IOException;

  /**
   * List all the consumer group offsets within a stream and group id.
   *
   * @param streamPath
   * @return list of consumer groups for the stream.
   */
  Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String streamPath,
        String groupId)
        throws IOException, IllegalArgumentException;

  /**
   * Alter the consumer group offsets for group with group id.
   *
   * @param groupId
   * @return list of consumer groups for the stream.
   */
  Map<TopicPartition, Errors> alterConsumerGroupOffsets(
      String streamPath,
      String groupId,
      Map<TopicPartition, OffsetAndMetadata> offsets)
      throws IOException, IllegalArgumentException;

    /**
   * Override {@link AutoCloseable#close()} to avoid declaring a checked
   * exception.
   */
  void close();

  /**
   * @param duration
   * @param unit
   */
  void close(long duration, TimeUnit unit);

  /**
   * @param duration
   */
  void close(Duration duration);
}
