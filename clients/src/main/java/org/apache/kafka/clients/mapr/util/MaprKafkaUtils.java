package org.apache.kafka.clients.mapr.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class MaprKafkaUtils {


    /**
    * Currently deciding which branch to load by {@link CommonClientConfigs#USE_BROKERS_CONFIG}.
    * By default it is false, meaning that MapR Client will be loaded. Users need to explicitly set it to true
    * to load kafka clients in Apache mode.
    */
    public static boolean isMapr(Map<?, ?> config) {
        Object useBrokers = config.get(CommonClientConfigs.USE_BROKERS_CONFIG);
        return !("true".equals(useBrokers) || Boolean.TRUE.equals(useBrokers));
    }

    public static boolean isMapr(Properties properties) {
        return isMapr(Utils.propsToMap(properties));
    }

    public static boolean isMapr(AbstractConfig config) {
        return isMapr(config.values());
    }

    // choose proper name here? as in listAllTopics we use this with not a default stream
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

    public static Set<String> maybeWrapDefaultStream(String defaultStream, Set<String> topics) {
        return topics.stream()
                .map(t -> maybeWrapDefaultStream(defaultStream, t))
                .collect(Collectors.toSet());
    }

    public static List<String> maybeWrapDefaultStream(String defaultStream, List<String> topics) {
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

    public static String maybeTrimTopic(String topic) {
        if (topic != null && topic.contains(":"))
            return topic.split(":")[1];
        else
            return topic;
    }

    /**
     * Lists all topics in provided default stream and in each of used streams in provided topics collection.
     * All topics to be returned are full-named (/stream:topic)
     */
    public static Set<String> listAllTopics(Admin adminClient, String defaultStream, Collection<String> topics) {
        Set<String> result = new HashSet<>();

        try {
            if (defaultStream != null && !defaultStream.isEmpty()) {
                result.addAll(maybeWrapDefaultStream(defaultStream, adminClient.listTopics().names().get(60, TimeUnit.SECONDS)));
            }
            else if (topics.stream().anyMatch(t -> !t.contains(":")))
                throw new KafkaException("Encountered short-named topic while default stream is not provided");

            Set<String> usedStreams = topics.stream()
                    .filter(t -> t.contains(":")).map(t -> t.split(":")[0]).collect(Collectors.toSet());
            for (String stream: usedStreams) {
                result.addAll(maybeWrapDefaultStream(stream, adminClient.listTopics(stream).names().get(60, TimeUnit.SECONDS)));
            }

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }
        return result;
    }
}
