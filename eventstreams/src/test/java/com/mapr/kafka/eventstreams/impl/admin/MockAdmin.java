package com.mapr.kafka.eventstreams.impl.admin;

import com.mapr.db.exceptions.TableNotFoundException;
import com.mapr.kafka.eventstreams.StreamDescriptor;
import com.mapr.kafka.eventstreams.TimestampType;
import com.mapr.kafka.eventstreams.TopicDescriptor;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;

public class MockAdmin implements com.mapr.kafka.eventstreams.Admin {

    private Map<String, List<String>> existingStreamsAndTopics = new HashMap<>();
    private Map<String, Integer> topicPartitions = new HashMap<>();

    public void setExistingStreamsAndTopics(List<String> topicsFullName) {
        existingStreamsAndTopics = new HashMap<>();
        for (String fullTopic : topicsFullName) {
            String[] tokens = fullTopic.split(":");
            String stream;
            String topic;

            if (tokens.length == 2) {
                stream = tokens[0];
                topic = tokens[1];

                existingStreamsAndTopics.compute(stream, (k,v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(topic);
                    return v;
                });
            } else {
                stream = tokens[0];
                existingStreamsAndTopics.putIfAbsent(stream, new ArrayList<>());
            }
        }
    }

    public void setExistingStreamsAndTopics(Map<String, Integer> topicsFullNameWithDescr) {
        existingStreamsAndTopics = new HashMap<>();
        topicPartitions = new HashMap<>();
        for (Map.Entry<String, Integer> topic : topicsFullNameWithDescr.entrySet()) {
            String[] tokens = topic.getKey().split(":");
            String streamName;
            String topicName;

            if (tokens.length == 2) {
                streamName = tokens[0];
                topicName = tokens[1];

                existingStreamsAndTopics.compute(streamName, (k,v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(topicName);
                    return v;
                });

                topicPartitions.put(topic.getKey(), topic.getValue());
            } else {
                streamName = tokens[0];
                existingStreamsAndTopics.putIfAbsent(streamName, new ArrayList<>());
            }
        }
    }

    @Override
    public void createStream(String streamPath, StreamDescriptor desc) throws IOException, IllegalArgumentException {

    }

    @Override
    public void editStream(String streamPath, StreamDescriptor desc) throws IOException, IllegalArgumentException {

    }

    @Override
    public StreamDescriptor getStreamDescriptor(String streamPath) throws IOException, IllegalArgumentException {
        return null;
    }

    @Override
    public void deleteStream(String streamPath) throws IOException, IllegalArgumentException {

    }

    @Override
    public int countTopics(String streamPath) throws IOException, IllegalArgumentException {
        return 0;
    }

    @Override
    public void createTopic(String streamPath, String topicName) throws IOException {

    }

    @Override
    public void createTopic(String streamPath, String topicName, int npartitions) throws IOException {
        if (!existingStreamsAndTopics.containsKey(streamPath)) {
            throw new TableNotFoundException();
        }

        if (existingStreamsAndTopics.get(streamPath).contains(topicName)) {
            String topicFullName = streamPath + ":" + topicName;
            throw new FileAlreadyExistsException("Topic " + topicFullName + " already exists.");
        }

        existingStreamsAndTopics.compute(streamPath, (k, v)
                -> {
            v.add(topicName);
            return v;
        });
    }

    @Override
    public void createTopic(String streamPath, String topicName, TopicDescriptor desc) throws IOException {

    }

    @Override
    public void editTopic(String streamPath, String topicName, int npartitions) throws IOException {
        if (!existingStreamsAndTopics.containsKey(streamPath)) {
            throw new TableNotFoundException("Stream " + streamPath +
                    " does not exist.");
        }

        if (!existingStreamsAndTopics.get(streamPath).contains(topicName)) {
            String topicFullName = streamPath + ":" + topicName;
            throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
        }
    }

    @Override
    public void editTopic(String streamPath, String topicName, TopicDescriptor desc) throws IOException {

    }

    @Override
    public void deleteTopic(String streamPath, String topicName) throws IOException {
        if (!existingStreamsAndTopics.containsKey(streamPath)) {
            throw new TableNotFoundException("Stream " + streamPath +
                    " does not exist.");
        }

        if (!existingStreamsAndTopics.get(streamPath).contains(topicName)) {
            String topicFullName = streamPath + ":" + topicName;
            throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
        }

        existingStreamsAndTopics.get(streamPath).remove(topicName);
    }

    @Override
    public void compactTopicNow(String streamPath, String topicName) throws IOException {

    }

    @Override
    public TopicDescriptor getTopicDescriptor(String streamPath, String topicName) throws IOException {
        if (!existingStreamsAndTopics.containsKey(streamPath)) {
            throw new TableNotFoundException("Stream " + streamPath +
                    " does not exist.");
        }

        if (!existingStreamsAndTopics.get(streamPath).contains(topicName)) {
            String topicFullName = streamPath + ":" + topicName;
            throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
        }

        String fullTopicName = streamPath + ":" + topicName;
        MTopicDescriptor desc = new MTopicDescriptor();
        desc.setPartitions(topicPartitions.get(fullTopicName));
        desc.setTimestampType(TimestampType.CREATE_TIME);
        return desc;
    }

    @Override
    public List<String> listTopics(String streamPath) throws IOException, IllegalArgumentException {
        if (!existingStreamsAndTopics.containsKey(streamPath)) {
            throw new TableNotFoundException("Stream " + streamPath +
                    " does not exist.");
        }
        return existingStreamsAndTopics.get(streamPath);
    }

    @Override
    public Map<String, List<TopicFeedInfo>> listTopicsForStream(String streamPath) throws IOException, IllegalArgumentException {
        if (!existingStreamsAndTopics.containsKey(streamPath)) {
            throw new TableNotFoundException("Stream " + streamPath +
                    " does not exist.");
        }

        Map<String, List<TopicFeedInfo>> topicStatMap = new HashMap<>();
        for (String topic : existingStreamsAndTopics.get(streamPath)) {
            String fullTopicName = streamPath + ":" + topic;

            List<TopicFeedInfo> fstatList = new ArrayList<TopicFeedInfo>();
            for (int i = 0; i < topicPartitions.get(fullTopicName); i++) {
                fstatList.add(new TopicFeedInfo(i));
            }
            topicStatMap.put(topic, fstatList);
        }

        return topicStatMap;
    }

    @Override
    public Collection<Object> listConsumerGroups(String streamPath) throws IOException, IllegalArgumentException {
        return null;
    }

    @Override
    public boolean streamExists(String streamPath) throws IOException {
        return false;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(
        final String streamPath, final String groupId)
        throws IOException, IllegalArgumentException {
        return null;
    }

    @Override
    public Map<TopicPartition, Errors> alterConsumerGroupOffsets(
        final String streamPath,
        final String groupId,
        final Map<TopicPartition, OffsetAndMetadata> offsets)
        throws IOException, IllegalArgumentException {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void close(long duration, TimeUnit unit) {

    }

    @Override
    public void close(Duration duration) {

    }
}
