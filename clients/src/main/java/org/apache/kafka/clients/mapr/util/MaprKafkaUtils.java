package org.apache.kafka.clients.mapr.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class MaprKafkaUtils {

    /*
    public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";
    public static final String LEAGAL_FULLTOPICNAME_PATTERN =
            String.format("(\\/%s+)+\\:%s+", LEGAL_CHARS, LEGAL_CHARS);

    public static void validateFullTopicName(String fullTopicName){
        if(!fullTopicName.matches(LEAGAL_FULLTOPICNAME_PATTERN)){
            throw new InvalidTopicException(String.format(
                    "Full topic name %s is invalid. It should be %s",
                    fullTopicName,
                    LEAGAL_FULLTOPICNAME_PATTERN));
        }
    }
    public static boolean isFullTopicName(String fullTopicName){
        return fullTopicName.startsWith("/") && fullTopicName.contains(":");
    }
    public static String buildFullTopicName(String streamName, String shortTopicName){
        return String.format("%s:%s", streamName, shortTopicName);
    }

    public static List<String> decorateTopicsWithDefaultStreamIfNeeded(List<String> topics, String defaultStream){
        List<String> res = new ArrayList<>(topics.size());
        for(String topic : topics){
            String decoratedTopic = topic;
            if(!topic.contains(":")){
                if(defaultStream.isEmpty()){
                    throw new InvalidTopicException(String.format(
                            "Default stream is not specified. Short topic name %s is invalid.",
                            topic));
                }
                decoratedTopic = String.format("%s:%s", defaultStream, topic);
            }
            validateFullTopicName(decoratedTopic);
            res.add(decoratedTopic);
        }

        return res;
    }

    public static List<String> addStreamNameToTopics(final List<String> topics, final String stream){
        final List<String> res = new LinkedList<>();
        for(String topic : topics){
            res.add(MapRTopicUtils.buildFullTopicName(stream, topic));
        }
        return res;
    }

    public static Map<String, Set<String>> groupTopicsByStreamName(List<String> topics){
        Map<String, Set<String>> res = new HashMap<>();
        for(String topic : topics){
           String[] parts = topic.split(":");
           String streamName = parts[0];
           String shortTopicName = parts[1];
           Set<String> groupedTopics = res.get(streamName);
           if(groupedTopics == null){
               groupedTopics = new HashSet<>();
               res.put(streamName, groupedTopics);
           }
           groupedTopics.add(shortTopicName);
        }

        return res;
    }

    public static Map<String, Set<String>> allTopicsForStreamSet(Set<String> streamSet) {
        return allTopicsForStreamSet(streamSet, AdminClient.create(new Properties()));
    }

    public static Map<String, Set<String>> allTopicsForStreamSet(Set<String> streamSet,
                                                                 AdminClient adminClient){
        Map<String, Set<String>> res = new HashMap<>();

        try {
            for (String streamName : streamSet) {
                res.put(streamName,
                        adminClient.listTopics(streamName).names().get(60, TimeUnit.SECONDS));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new KafkaException(e);
        } finally {
            adminClient.close();
        }

        return res;
    }
    */


    /**
    * Currently deciding which branch to load by {@link CommonClientConfigs#USE_BROKERS_CONFIG}.
    * By default it is false, meaning that MapR Client will be loaded. Users need to explicitly set it to true
    * to load kafka clients in Apache mode.
    */
    public static boolean isMapr(Map<String, ?> config) {
        Object useBrokers = config.get(CommonClientConfigs.USE_BROKERS_CONFIG);
        return !("true".equals(useBrokers) || Boolean.TRUE.equals(useBrokers));
    }

    public static boolean isMapr(Properties properties) {
        return isMapr(Utils.propsToMap(properties));
    }

    public static boolean isMapr(AbstractConfig config) {
        return isMapr(config.values());
    }

    public static String maybeWrapDefaultStream(String defaultStream, String topic) {
        if (topic.contains("/") || topic.contains(":")) {
            return topic;
        }
        if (defaultStream == null || defaultStream.isEmpty()) {
            throw new KafkaException("MapR kafka clients cannot work with topics without a stream. " +
                            "Please either specify default stream or add a stream name to the topic name.");
        }
        return defaultStream + ":" + topic;
    }

    public static Collection<String> maybeWrapDefaultStream(String defaultStream, Collection<String> topics) {
        return topics.stream()
                .map(t -> maybeWrapDefaultStream(defaultStream, t))
                .collect(Collectors.toList());
    }

    public static Collection<TopicPartition> maybeWrapDefaultStreamPartitions(String defaultStream, Collection<TopicPartition> partitions) {
        partitions.forEach(p -> p.setTopic(maybeWrapDefaultStream(defaultStream, p.topic())));
        return partitions;
    }

    public static <T> Map<TopicPartition, T> maybeWrapDefaultStreamPartitions(String defaultStream, Map<TopicPartition, T> partitions) {
        partitions.keySet().forEach(p -> p.setTopic(maybeWrapDefaultStream(defaultStream, p.topic())));
        return partitions;
    }
}
