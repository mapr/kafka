package com.mapr.kafka.eventstreams.impl.listener;

import com.mapr.fs.jni.NativeData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.ArrayList;
import java.util.List;

public class NativeDataParserV10 extends NativeDataParser {

  public NativeDataParserV10(NativeData nativeData) {
    super(nativeData);
  }

  @Override
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
      RecordHeaders headers = new RecordHeaders();
      for (int j = 0; j < numHeaders; j++) {
        int hKeySz = (int)getLongData();
        int hKeyOff = (int)getLongData();
        int hValSz = (int)getLongData();
        int hValOff = (int)getLongData();
        String hKey = new String(nativeData.byte_data, hKeyOff, hKeySz);
        byte [] hVal = null;
        if (hValSz > 0) {
          hVal = new byte[hValSz];
          System.arraycopy(nativeData.byte_data, hValOff, hVal, 0, hValSz);
        }
        RecordHeader header = new RecordHeader(hKey, hVal);
        headers.add(header);
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
          key, value, headers, producer));
    }
    return recs;
  }

}
