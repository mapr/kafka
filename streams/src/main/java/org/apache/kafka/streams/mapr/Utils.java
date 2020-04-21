package org.apache.kafka.streams.mapr;

import com.mapr.fs.MapRFileAce;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.PermissionNotMatchException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.mapr.tools.KafkaMaprStreams;
import org.apache.kafka.mapr.tools.KafkaMaprTools;
import org.apache.kafka.mapr.tools.KafkaMaprfs;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @deprecated - use mapr-eco-tools utils instead
 */
@SuppressWarnings("unused")
public class Utils {
    public static void createAppDirAndInternalStreamsForKafkaStreams(StreamsConfig config) {
        KafkaStreamsInternalStorageInitializer.createAppDirAndInternalStreams(config);
    }

    @Deprecated
    public static void enableLogCompactionForStreamIfNotEnabled(String streamName) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            maprStreams.ensureStreamLogCompactionIsEnabled(streamName);
        }
    }

    @Deprecated
    public static void validateDirectoryPerms(FileSystem fs, String path, String user, String errorMsg) {
        final KafkaMaprfs maprfs = KafkaMaprTools.tools().maprfs();
        if (!maprfs.isAccessibleAsDirectory(path)) {
            throw new KafkaException(new PermissionNotMatchException(errorMsg));
        }
    }

    @Deprecated
    public static boolean streamExists(String streamName) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            return maprStreams.streamExists(streamName);
        }
    }

    @Deprecated
    public static String getShortTopicNameFromFullTopicName(final String fullTopicName) {
        return KafkaMaprStreams.getShortTopicNameFromFullTopicName(fullTopicName);
    }

    @Deprecated
    public static void createStream(String streamName) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            maprStreams.createStreamForClusterAdmin(streamName);
        }
    }

    @Deprecated
    public static void createStreamWithPublicPerms(String streamName) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            maprStreams.createStreamForAllUsers(streamName);
        }
    }

    @SuppressWarnings("RedundantThrows")
    @Deprecated
    public static boolean maprFSpathExists(FileSystem fs,
                                           String path) throws IOException {
        return KafkaMaprTools.tools().maprfs().exists(path);
    }

    @Deprecated
    public static void maprFSpathCreate(FileSystem fs,
                                        String pathStr,
                                        ArrayList<MapRFileAce> aces,
                                        String currentUser,
                                        String errorMsg) throws IOException {
        KafkaMaprfs maprfs = KafkaMaprTools.tools().maprfs();
        maprfs.mkdirs(pathStr);
        try {
            maprfs.setAces(pathStr, aces);
        } catch (KafkaException e) {
            if (!maprfs.isAccessibleAsDirectory(pathStr)) {
                throw new PermissionNotMatchException(errorMsg);
            }
        }
    }
}
