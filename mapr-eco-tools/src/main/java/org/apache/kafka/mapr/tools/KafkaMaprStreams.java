package org.apache.kafka.mapr.tools;

import com.mapr.kafka.eventstreams.Admin;
import com.mapr.kafka.eventstreams.StreamDescriptor;
import com.mapr.kafka.eventstreams.Streams;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.KafkaException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

public class KafkaMaprStreams implements Closeable {
    private final Admin admin;
    public static final String PUBLIC_PERMISSIONS = "p";

    KafkaMaprStreams(Admin admin) {
        this.admin = admin;
    }

    public void createStreamForClusterAdmin(String streamName) {
        createStreamWithPerms(streamName, null, null);
    }

    public void createStreamForCurrentUser(String streamName) {
        String currentUserPerms = buildPermsForCurrentUser();
        createStreamWithPerms(streamName, currentUserPerms, currentUserPerms);
    }

    public void createStreamForAllUsers(String streamName) {
        createStreamWithPerms(streamName, PUBLIC_PERMISSIONS, PUBLIC_PERMISSIONS);
    }

    private String buildPermsForCurrentUser() {
        String clusterAdminUser = KafkaMaprTools.tools().getClusterAdminUserName();
        String currentUser = KafkaMaprTools.tools().getCurrentUserName();
        if (currentUser.equals(clusterAdminUser)) {
            return null;
        } else {
            return "u:" + clusterAdminUser + " | u:" + currentUser;
        }
    }

    private StreamDescriptor createDescriptorWithPerms(String producerPerms, String consumerPerms){
        StreamDescriptor streamDescriptor = Streams.newStreamDescriptor();
        if (producerPerms != null) {
            streamDescriptor.setProducePerms(producerPerms);
        }
        if (consumerPerms != null) {
            streamDescriptor.setConsumePerms(consumerPerms);
        }
        return streamDescriptor;
    }

    public void setStreamPerms(String streamName, String producerPerms, String consumerPerms) {
        try {
            StreamDescriptor desc = createDescriptorWithPerms(producerPerms, consumerPerms);
            admin.editStream(streamName, desc);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public void createStreamWithPerms(String streamName, String producerPerms, String consumerPerms) {
        try {
            StreamDescriptor desc = createDescriptorWithPerms(producerPerms, consumerPerms);
            admin.createStream(streamName, desc);
        } catch (Exception e) {
            if (!streamExists(streamName)) {
                throw new KafkaException(e);
            }
        }
    }

    public boolean streamHasPerms(String streamName, String producerPerm, String consumerPerm) {
        try {
            StreamDescriptor desc = admin.getStreamDescriptor(streamName);
            return (Objects.equals(producerPerm, desc.getProducePerms())
                && Objects.equals(consumerPerm, desc.getConsumePerms()));
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public boolean streamExists(String streamName) {
        try {
            return admin.streamExists(streamName);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static String getShortTopicNameFromFullTopicName(final String fullTopicName) {
        return StringUtils.substringAfter(fullTopicName, ":");
    }

    /**
     * The method enables logCompaction for the stream and disables TTL.
     * Unlike Apache Kafka, MapR Streams allows both
     * TTL and LogCompaction being enabled
     **/
    public void ensureStreamLogCompactionIsEnabled(String streamName) {
        try {
            StreamDescriptor desc = admin.getStreamDescriptor(streamName);
            if (!desc.getCompact()) {
                desc.setCompact(true);
                desc.setTimeToLiveSec(0);
                admin.editStream(streamName, desc);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
