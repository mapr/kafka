package org.apache.kafka.streams.mapr;

import com.mapr.streams.Admin;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.mapr.InternalStreamNotExistException;

import java.io.IOException;

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
    public static void createInternalStreamsIfNotExist() {
        try {
            if (!maprFSpathExists(StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER)) {
                throw new KafkaException(StreamsConfig.STREAMS_INTERNAL_STREAM_COMMON_FOLDER + " doesn't exist");
            }
            if (!maprFSpathExists(StreamsConfig.STREAMS_INTERNAL_STREAM_FOLDER)) {
                maprFSpathCreate(StreamsConfig.STREAMS_INTERNAL_STREAM_FOLDER);
            }
            if (!streamExists(StreamsConfig.STREAMS_INTERNAL_STREAM_NOTCOMPACTED)) {
                createStream(StreamsConfig.STREAMS_INTERNAL_STREAM_NOTCOMPACTED);
            }
            if (!streamExists(StreamsConfig.STREAMS_INTERNAL_STREAM_COMPACTED)) {
                createStream(StreamsConfig.STREAMS_INTERNAL_STREAM_COMPACTED);
            }
            if(!streamExists(StreamsConfig.STREAMS_CLI_SIDE_ASSIGNMENT_INTERNAL_STREAM)){
                throw new InternalStreamNotExistException(StreamsConfig.STREAMS_CLI_SIDE_ASSIGNMENT_INTERNAL_STREAM + " doesn't exist");
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

    private static void createStream(String streamName) {
        try {
            Configuration conf = new Configuration();
            Admin admin = Streams.newAdmin(conf);
            StreamDescriptor desc = Streams.newStreamDescriptor();
            admin.createStream(streamName, desc);
        } catch (IOException e){
            throw new KafkaException(e);
        }
    }

    private static boolean maprFSpathExists(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    private static void maprFSpathCreate(String path) throws IOException {
        fs.mkdirs(new Path(path));
    }

    public static String getShortTopicNameFromFullTopicName(final String fullTopicName){
        String [] arr = fullTopicName.split(":");
        return (arr.length > 1) ? arr[1] : arr[0];
    }
}
