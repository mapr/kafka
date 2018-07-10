package org.apache.kafka.clients.mapr.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MapRTopicUtils {

    public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";
    public static final String LEAGAL_FULLTOPICNAME_PATTERN =
            String.format("\\/%s+\\:%s+", LEGAL_CHARS, LEGAL_CHARS);

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

        for(String streamName : streamSet){
            try {
                res.put(streamName,
                        adminClient.listTopics(streamName).names().get(60, TimeUnit.SECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                new KafkaException(e);
            } finally {
                adminClient.close();
            }
        }

        return res;
    }
}
