package org.apache.kafka.clients.mapr.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    private static final Logger LOG = LoggerFactory.getLogger(MaprKafkaUtils.class);

    private static final boolean IS_OS_WINDOWS = System.getProperty("os.name").toLowerCase().startsWith("windows");
    public static final String MAPR_HOME = findMapRHome();
    public static final String MAPR_CLUSTERS_FILE = MAPR_HOME + "/conf/mapr-clusters.conf";
    private static List<String> clusterNames = null;

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

    public static String findMapRHome() {
        String maprHome = System.getenv("MAPR_HOME");
        if (maprHome == null) {
            LOG.warn("Environment variable MAPR_HOME is null");
            maprHome = System.getProperty("mapr.home.dir");
            if (maprHome == null) {
                LOG.warn("System property mapr.home.dir is null");
                maprHome = IS_OS_WINDOWS ? "C:/opt/mapr" : "/opt/mapr";
                LOG.warn("Setting MapR home as {} by default", maprHome);
            }
        }
        return maprHome;
    }

    public static List<String> listClusterNames() {
        try {
            if (clusterNames == null) {
                clusterNames = Files.lines(Paths.get(MAPR_CLUSTERS_FILE))
                        .map(l -> l.split("\\s")[0]).collect(Collectors.toList());
            }
            return clusterNames;
        } catch (IOException e) {
            throw new KafkaException("Could not listClusterNames from " + MAPR_CLUSTERS_FILE, e);
        }
    }

}
