package org.apache.kafka.streams.mapr;

import com.mapr.fs.AceHelper;
import com.mapr.fs.MapRFileAce;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.proto.Common;
import com.mapr.streams.Admin;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.PermissionNotMatchException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.mapr.InternalStreamNotExistException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    /**
     * The method creates internal streams (without log compaction and with log compaction)
     * and appropriate paths if they don't exist.
     *
     */
    public static void createAppDirAndInternalStreamsIfNotExist(StreamsConfig config) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            if (!maprFSpathExists(fs, StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER)) {
                throw new KafkaException(StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER + " doesn't exist");
            }
            String currentUser = UserGroupInformation.getCurrentUser().getUserName();
            if (!maprFSpathExists(fs, config.getStreamsInternalStreamFolder())) {
                // Creation of application forler with appropriate aces
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
                ace = new MapRFileAce(MapRFileAce.AccessType.DELETECHILD);
                ace.setBooleanExpression("u:" + currentUser);
                aceList.add(ace);

                maprFSpathCreate(fs, config.getStreamsInternalStreamFolder(), aceList);
            }else {
                String errorMessage = "User: "
                        + currentUser
                        + " has no permissions to run KStreams application with ID: "
                        + config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
                validateDirectoryPerms(fs, config.getStreamsInternalStreamFolder(), currentUser, errorMessage);
            }
            if (!streamExists(config.getStreamsInternalStreamNotcompacted())) {
                createStream(config.getStreamsInternalStreamNotcompacted());
            }
            if (!streamExists(config.getStreamsInternalStreamCompacted())) {
                createStream(config.getStreamsInternalStreamCompacted());
            }
            enableLogCompactionForStreamIfNotEnabled(config.getStreamsInternalStreamCompacted());

            if(!streamExists(config.getStreamsCliSideAssignmentInternalStream())){
                throw new InternalStreamNotExistException(config.getStreamsCliSideAssignmentInternalStream() + " doesn't exist");
            }
        }catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static void enableLogCompactionForStreamIfNotEnabled(String streamName){
        try {
            Configuration conf = new Configuration();
            Admin admin = Streams.newAdmin(conf);
            StreamDescriptor desc = admin.getStreamDescriptor(streamName);
            if(!desc.getCompact()) {
                desc.setCompact(true);
                admin.editStream(streamName, desc);
            }
        } catch (IOException e){
            throw new KafkaException(e);
        }
    }

    private static boolean validatePermsHelper(MapRFileAce ace,
                                               String userBoolExpr){
        try {
            String[] boolExprs = ace.getBooleanExpression().split(",");
            for (String boolExpr : boolExprs) {
                if ((AceHelper.toPostfix(boolExpr)).equals(userBoolExpr)) {
                    return true;
                }
            }
        } catch(IOException e){
            throw new KafkaException(e);
        }
        return false;
    }

    public static void validateDirectoryPerms(FileSystem fs, String path, String user, String errorMsg){
        try {
            List<MapRFileAce> aces = ((MapRFileSystem) fs).getAces(new Path(path));
            boolean readDirAce = false;
            boolean addChild = false;
            boolean lookupDir = false;
            boolean deleteChild = false;
            String userBoolExpr = AceHelper.toPostfix(String.format("u:%s", user));
            for(MapRFileAce ace : aces) {
                boolean userHasPerms = validatePermsHelper(ace, userBoolExpr);
                if(ace.getAccessType().equals(MapRFileAce.AccessType.READDIR)){
                    readDirAce = userHasPerms;
                }
                if(ace.getAccessType().equals(MapRFileAce.AccessType.ADDCHILD)){
                    addChild = userHasPerms;
                }
                if(ace.getAccessType().equals(MapRFileAce.AccessType.LOOKUPDIR)){
                    lookupDir = userHasPerms;
                }
                if(ace.getAccessType().equals(MapRFileAce.AccessType.DELETECHILD)){
                    deleteChild = userHasPerms;
                }
            }
            boolean userHasAllNeededPerms = readDirAce && lookupDir && addChild && deleteChild;
            if(!userHasAllNeededPerms) {
                throw new PermissionNotMatchException(errorMsg);
            }
        } catch (IOException e){
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

    public static void createStream(String streamName) {
        try {
            Configuration conf = new Configuration();
            Admin admin = Streams.newAdmin(conf);
            StreamDescriptor desc = Streams.newStreamDescriptor();
            admin.createStream(streamName, desc);
        } catch (IOException e){
            throw new KafkaException(e);
        }
    }

    public static boolean maprFSpathExists(FileSystem fs, String path) throws IOException {
        return fs.exists(new Path(path));
    }

    public static void maprFSpathCreate(FileSystem fs, String pathStr, ArrayList<MapRFileAce> aces) throws IOException {
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
