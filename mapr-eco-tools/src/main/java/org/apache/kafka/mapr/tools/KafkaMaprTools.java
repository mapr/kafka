package org.apache.kafka.mapr.tools;

import com.mapr.fs.MapRFileSystem;
import com.mapr.kafka.eventstreams.Admin;
import com.mapr.kafka.eventstreams.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;

public class KafkaMaprTools {
    protected static KafkaMaprTools tools = new KafkaMaprTools();

    public static KafkaMaprTools tools() {
        return tools;
    }

    public KafkaMaprfs maprfs() {
        return new KafkaMaprfs(getMapRFileSystem());
    }

    public KafkaMaprStreams streams() {
        return new KafkaMaprStreams(newStreamsAdmin());
    }

    public MapRFileSystem getMapRFileSystem() {
        try {
            return (MapRFileSystem) FileSystem.get(new Configuration());
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public Admin newStreamsAdmin() {
        try {
            return Streams.newAdmin(new Configuration());
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public String getCurrentUserName() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public String getClusterAdminUserName() {
        try {
            return UserGroupInformation.getLoginUser().getUserName();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
