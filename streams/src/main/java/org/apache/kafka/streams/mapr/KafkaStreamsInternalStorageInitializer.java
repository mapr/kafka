package org.apache.kafka.streams.mapr;

import com.mapr.fs.MapRFileAce;
import org.apache.hadoop.io.PermissionNotMatchException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.mapr.tools.KafkaMaprStreams;
import org.apache.kafka.mapr.tools.KafkaMaprfs;
import org.apache.kafka.mapr.tools.KafkaMaprTools;
import org.apache.kafka.mapr.tools.MaprfsPermissions;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.mapr.InternalStreamNotExistException;

import java.io.IOException;

public class KafkaStreamsInternalStorageInitializer {

    public static void createAppDirAndInternalStreams(StreamsConfig config) {
        try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
            createAppDir(config);
            createInternalStreams(maprStreams, config);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    private static void createAppDir(StreamsConfig config) throws IOException {
        KafkaMaprfs maprfs = KafkaMaprTools.tools().maprfs();
        maprfs.requireExisting(StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER);

        String internalFolder = config.getStreamsInternalStreamFolder();
        if (!maprfs.exists(internalFolder)) {
            maprfs.mkdirs(internalFolder);
            maprfs.setPermissions(internalFolder, MaprfsPermissions.permissions()
                    .put(MapRFileAce.AccessType.READDIR, MaprfsPermissions.PUBLIC)
                    .put(MapRFileAce.AccessType.LOOKUPDIR, MaprfsPermissions.PUBLIC)
                    .put(MapRFileAce.AccessType.ADDCHILD, MaprfsPermissions.STARTUP_USER)
                    .put(MapRFileAce.AccessType.DELETECHILD, MaprfsPermissions.STARTUP_USER)
                    .put(MapRFileAce.AccessType.WRITEFILE, MaprfsPermissions.STARTUP_USER)
                    .loadFromConfig(config.getString(StreamsConfig.APPLICATION_DIR_ACES_CONFIG)));
        }

        if (!maprfs.isAccessibleAsDirectory(internalFolder)) {
            String user = KafkaMaprTools.tools().getCurrentUserName();
            String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
            throw new PermissionNotMatchException(String.format(
                    "User '%s' has no permissions to run KStreams application with ID '%s'", user, applicationId
            ));
        }
    }

    private static void createInternalStreams(KafkaMaprStreams maprStreams, StreamsConfig config) {
        maprStreams.createStreamForClusterAdmin(config.getStreamsInternalStreamNotcompacted());
        maprStreams.createStreamForCurrentUser(config.getStreamsInternalStreamCompacted());
        maprStreams.ensureStreamLogCompactionIsEnabled(config.getStreamsInternalStreamCompacted());

        String cliSideAssignmentInternalStream = config.getStreamsCliSideAssignmentInternalStream();
        if (!maprStreams.streamExists(cliSideAssignmentInternalStream)) {
            throw new InternalStreamNotExistException(cliSideAssignmentInternalStream + " doesn't exist");
        }
    }
}
