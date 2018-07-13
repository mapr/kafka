package org.apache.kafka.streams.mapr;

import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.proto.Common;
import com.mapr.streams.Admin;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import org.apache.hadoop.fs.permission.AclEntry;
import com.mapr.fs.MapRFileAce;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.mapr.InternalStreamNotExistException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Utils {

    public static FileSystem fs;

    static {
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
        }catch (IOException e){
            throw new KafkaException(e);
        }
    }

    /**
     * The method creates internal streams (without log compaction and with log compaction)
     * and appropriate paths if they don't exist.
     *
     */
    public static void createAppDirAndInternalStreamsIfNotExist(StreamsConfig config) {
        try {
            if (!maprFSpathExists(StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER)) {
                throw new KafkaException(StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER + " doesn't exist");
            }
            if (!maprFSpathExists(config.getStreamsInternalStreamFolder())) {
                // Creation of application forler with appropriate aces
                String currentUser = System.getProperty("user.name");
                ArrayList<MapRFileAce> aceList = new ArrayList<MapRFileAce>();

                MapRFileAce ace = new MapRFileAce(MapRFileAce.AccessType.READDIR);
                ace.setBooleanExpression("u:" + currentUser);
                aceList.add(ace);
                ace = new MapRFileAce(MapRFileAce.AccessType.ADDCHILD);
                ace.setBooleanExpression("u:" + currentUser);
                aceList.add(ace);
                ace = new MapRFileAce(MapRFileAce.AccessType.LOOKUPDIR);
                ace.setBooleanExpression("u:" + currentUser);
                aceList.add(ace);

                maprFSpathCreate(config.getStreamsInternalStreamFolder(), aceList);
            }
            if (!streamExists(config.getStreamsInternalStreamNotcompacted())) {
                createStream(config.getStreamsInternalStreamNotcompacted(), false);
            }
            if (!streamExists(config.getStreamsInternalStreamCompacted())) {
                createStream(config.getStreamsInternalStreamCompacted(), true);
            }
            if(!streamExists(config.getStreamsCliSideAssignmentInternalStream())){
                throw new InternalStreamNotExistException(config.getStreamsCliSideAssignmentInternalStream() + " doesn't exist");
            }
        }catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static boolean streamExists(String streamName){
        try {
            Configuration conf = new Configuration();
            Admin admin = Streams.newAdmin(conf);
            return admin.streamExists(streamName);
        } catch (IOException e){
            throw new KafkaException(e);
        }
    }

    public static void createStream(String streamName, boolean logCompactionEnabled) {
        try {
            Configuration conf = new Configuration();
            Admin admin = Streams.newAdmin(conf);
            StreamDescriptor desc = Streams.newStreamDescriptor();
            admin.createStream(streamName, desc);
            if(logCompactionEnabled) {
                desc.setCompact(logCompactionEnabled);
                admin.editStream(streamName, desc);
            }
        } catch (IOException e){
            throw new KafkaException(e);
        }
    }

    public static boolean maprFSpathExists(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    public static void maprFSpathCreate(String pathStr, ArrayList<MapRFileAce> aces) throws IOException {
        Path path = new Path(pathStr);
        fs.mkdirs(path);

        // Set other aces
        ((MapRFileSystem)fs).setAces(path, aces);

        // Setting inherits to false
        int noinherit = 1;
        ((MapRFileSystem)fs).setAces(path, new ArrayList<Common.FileACE>(), false,
                noinherit, 0, false, null);
    }

    public static String getShortTopicNameFromFullTopicName(final String fullTopicName){
        String [] arr = fullTopicName.split(":");
        return (arr.length > 1) ? arr[1] : arr[0];
    }
}
