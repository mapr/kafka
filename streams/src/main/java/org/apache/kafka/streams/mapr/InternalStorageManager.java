package org.apache.kafka.streams.mapr;

import com.mapr.fs.MapRFileAce;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.mapr.tools.KafkaMaprStreams;
import org.apache.kafka.mapr.tools.KafkaMaprTools;
import org.apache.kafka.mapr.tools.KafkaMaprfs;
import org.apache.kafka.mapr.tools.MaprfsPermissions;
import org.apache.kafka.streams.StreamsConfig;


/**
 * Manages creation/deletion of mapr kafka-streams' internal application directory and internal streams:
 * - /apps/kafka-streams/<application_id>
 * - /apps/kafka-streams/<application_id>/kafka-internal-stream
 * - /apps/kafka-streams/<application_id>/kafka-internal-stream-compacted
 */

public class InternalStorageManager {

    public static class Storage {
        public final String appDir;
        public final String internalStream;
        public final String internalStreamCompacted;

        public Storage(String applicationId) {
            this.appDir = "/apps/kafka-streams/" + applicationId + "/";
            this.internalStream = appDir + "kafka-internal-stream";
            this.internalStreamCompacted = internalStream + "-compacted";
        }
    }

    public static Storage storage(String applicationId) {
        return new Storage(applicationId);
    }

    public static void create(StreamsConfig config) {
        Storage storage = storage(config.getString(StreamsConfig.APPLICATION_ID_CONFIG));
        createAppDir(storage, config);
        createInternalStreams(storage);
    }

    public static void delete(String applicationId) {
        Storage storage = storage(applicationId);
        deleteAppDir(storage);
        deleteInternalStreams(storage);
    }

    private static void createAppDir(Storage storage, StreamsConfig config) {
        try (KafkaMaprfs maprfs = KafkaMaprTools.tools().maprfs()) {
            maprfs.requireParentExisting(storage.appDir);

            if (!maprfs.exists(storage.appDir)) {
                maprfs.mkdirs(storage.appDir);
                maprfs.setPermissions(storage.appDir, MaprfsPermissions.permissions()
                        .put(MapRFileAce.AccessType.READDIR, MaprfsPermissions.PUBLIC)
                        .put(MapRFileAce.AccessType.LOOKUPDIR, MaprfsPermissions.PUBLIC)
                        .put(MapRFileAce.AccessType.ADDCHILD, MaprfsPermissions.STARTUP_USER)
                        .put(MapRFileAce.AccessType.DELETECHILD, MaprfsPermissions.STARTUP_USER)
                        .put(MapRFileAce.AccessType.WRITEFILE, MaprfsPermissions.STARTUP_USER)
                        .loadFromConfig(config.getString(StreamsConfig.APPLICATION_DIR_ACES_CONFIG)));
            }

            if (!maprfs.isAccessibleAsDirectory(storage.appDir)) {
                String user = KafkaMaprTools.tools().getCurrentUserName();
                String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
                throw new KafkaException(String.format(
                        "User '%s' has no permissions to run KStreams application with ID '%s'", user, applicationId
                ));
            }
        }
    }

    private static void createInternalStreams(Storage storage) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            if (!maprStreams.streamExists(storage.internalStream))
                maprStreams.createStreamForClusterAdmin(storage.internalStream);
            if (!maprStreams.streamExists(storage.internalStreamCompacted))
                maprStreams.createStreamForCurrentUser(storage.internalStreamCompacted);

            maprStreams.ensureStreamLogCompactionIsEnabled(storage.internalStreamCompacted);
        }
    }

    private static void deleteAppDir(Storage storage) {
        try (KafkaMaprfs fs = KafkaMaprTools.tools().maprfs()) {
            if(fs.exists(storage.appDir)){
                fs.deleteRecursive(storage.appDir);
            }
        }
    }

    private static void deleteInternalStreams(Storage storage) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            if (maprStreams.streamExists(storage.internalStream))
                maprStreams.deleteStream(storage.internalStream);
            if (maprStreams.streamExists(storage.internalStreamCompacted))
                maprStreams.deleteStream(storage.internalStreamCompacted);
        }
    }

}
