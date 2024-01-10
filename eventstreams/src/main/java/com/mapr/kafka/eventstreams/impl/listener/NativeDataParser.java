package com.mapr.kafka.eventstreams.impl.listener;

import com.mapr.fs.jni.Errno;
import com.mapr.fs.jni.NativeData;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to parse NativeData returned by jni layer.
 * It is right now used in two ways -
 * 1. To send back the list of subscriptions for this listener.
 * 2. To send back all the messages during a poll.
 */

public class NativeDataParser {
  protected NativeData nativeData;

  NativeDataParser(NativeData nativeData) {
    this.nativeData = nativeData;
    this.nativeData.startParser();
  }

  public boolean HasData() { return nativeData.HasData(); }

  /* 
   * Below is the content of the long_data array and byte_data array when these
   * arrays are used to return a list of TopicPartitions to the java layer. The
   * information we need per Topic partitio is - (a) stream name and its size
   * (b) topic name and its size (c) feed id. 
   *
   *   Format of the long array
   *   -----------------------------------------------------------------
   *   | Stream1_NameSz | Stream1_NameOf | Topic1_NameSz | Topic1_NameOf | Feed1_Id | Stream2_Name |....
   *   ----------------------------------------------------------------
   *
   *   Format of the actual byte array
   *   ------------------------------------------------------------
   *   |Stream1_Name | Topic1_Name | Stream2_Name | Topic2_Name |...
   *   -----------------------------------------------------------
   */

  public TopicPartition getNextTopicPartition() {
    int size = (int)getLongData();
    int offset = (int)getLongData();
    String stream = new String(nativeData.byte_data, offset, size);

    size = (int)getLongData();
    offset = (int)getLongData();
    String topic = new String(nativeData.byte_data, offset, size);

    int partition = (int)getLongData();

    return new TopicPartition(stream + ":" + topic, partition);
  }

  /*
   * Below is the content of the long_data array and byte_data array when they
   * are used to return poll result to the java land. While returning the poll
   * result the arrays first encode a TopicPartition followed by how many
   * messages are being returned for this partition and then a repeating
   * sequence of offset, key and value for each message. The long's array
   * contains all the sizes and the byte_array actually contains the key and
   * value. Once all the messages of a feed are done, then it is immediately
   * followed by the next feed.
   *
   *   Format of the long array
   *   -----------------------------------------------------------------
   *   | StNameSz | StNameOf | Tp1NameSz | Tp1NameOf | FeedId | error |
         NumMsg | seqNum | KeyOff | KeySz | ValOff | ValSz | seqNum | KeySz |
         KeyOff | ValSz | ValOff| ....| StNameSz | St2NameOff | T2NameSz |
         T2NameOff | Feed_Id | NumMsg |...
   *   ----------------------------------------------------------------
   *
   *   Format of the actual byte array
   *   ------------------------------------------------------------
   *   |Stream1_Name | Topic1_Name | key | val | key | val |.... | stream2_name| topic2_name| key | val | key | val | ...
   *   -----------------------------------------------------------
   */

  public Map<TopicPartition, List<ListenerRecord>> parseListenerRecords(boolean stripStreamPath)
      throws NoOffsetForPartitionException, RecordTooLargeException {

    Map<TopicPartition, List<ListenerRecord>> result = new HashMap<TopicPartition, List<ListenerRecord>> ();
    while (HasData()) {
      TopicPartition p = getNextTopicPartition();
      int error = (int)getLongData();
      if (error != 0) {
        if (error < 0)
          error = -error;
        if (error == Errno.E2BIG) {
            throw new RecordTooLargeException("There are some messages at " + p +
                                              " whose size is larger than the fetch size");
        } else if (error == Errno.EACCES) {
          throw new TopicAuthorizationException(p.topic());
        } else {
            throw new NoOffsetForPartitionException(p);
        }
      }
      long numMsgs = getLongData();

      String topicName = p.topic();
      if (stripStreamPath) {
        // Remove the stream-path from the topic-name
        String topicFullName = topicName;
        int idx = topicFullName.lastIndexOf(':');
        if (idx >= 0)
          topicName = topicFullName.substring(idx + 1);
      }

      List<ListenerRecord> recs = getListenerRecords(topicName, p, numMsgs);

      result.put(p, recs);
    }
    return result;
  }

  protected List<ListenerRecord> getListenerRecords(String topicName, TopicPartition p, long numMsgs) {
    List<ListenerRecord> recs = new ArrayList<ListenerRecord>();
    int prevProducerOff = 0;
    String prevProducer = null;

    for (int i = 0; i < numMsgs; i++) {
      long msgSeq =  getLongData();
      long timestamp =  getLongData();
      int timestampType = (int)getLongData();
      int keySz = (int)getLongData();
      int keyOff = (int)getLongData();
      int valSz = (int)getLongData();
      int valOff = (int)getLongData();
      int producerSz = (int)getLongData();
      int producerOff = (int)getLongData();
      int numHeaders = (int)getLongData();
      // Ignore the headers for Kafka-0.9 clients.
      for (int j = 0; j < numHeaders; j++) {
        nativeData.longArrIndex++; // Header Key Size
        nativeData.longArrIndex++; // Header Key Offset
        nativeData.longArrIndex++; // Header Val Size
        nativeData.longArrIndex++; // Header Val Offset
      }
      byte[] key = null;
      byte[] value = null;
      String producer = null;
      if (keySz > 0) {
        key = new byte[keySz];
        System.arraycopy(nativeData.byte_data, keyOff, key, 0, keySz);
      }
      if (valSz > 0) {
        value = new byte[valSz];
        System.arraycopy(nativeData.byte_data, valOff, value, 0, valSz);
      }
      if (producerSz > 0) {
        if (producerOff == prevProducerOff) {
          producer = prevProducer;
        } else {
          producer = new String(nativeData.byte_data,
              producerOff, producerSz);

          // avoid duplicates where possible
          prevProducerOff = producerOff;
          prevProducer = producer;
        }
      }

      recs.add(new ListenerRecord(topicName, p.partition(),
          msgSeq, timestamp, timestampType,
          key, value, new RecordHeaders(), producer));
    }
    return recs;
  }

  protected long getLongData() {
    return nativeData.long_data[nativeData.longArrIndex++];
  }
}
 
