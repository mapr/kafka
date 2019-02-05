package org.apache.kafka.mapr.tools;

import com.mapr.streams.Admin;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.KafkaException;

import java.io.Closeable;
import java.io.IOException;

public class KafkaMaprStreams implements Closeable {
    private final Admin admin;

    KafkaMaprStreams(Admin admin) {
        this.admin = admin;
    }

    public void createStreamForClusterAdmin(String streamName) {
        createStreamWithPerms(streamName, null);
    }

    public void createStreamForCurrentUser(String streamName) {
        createStreamWithPerms(streamName, buildPermsForCurrentUser());
    }

    public void createStreamForAllUsers(String streamName) {
        createStreamWithPerms(streamName, "p");
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

    private void createStreamWithPerms(String streamName, String perms) {
        try {
            StreamDescriptor desc = Streams.newStreamDescriptor();
            if (perms != null) {
                desc.setConsumePerms(perms);
                desc.setProducePerms(perms);
            }
            admin.createStream(streamName, desc);
        } catch (Exception e) {
            if (!streamExists(streamName)) {
                throw new KafkaException(e);
            }
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
