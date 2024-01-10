package com.mapr.kafka.eventstreams.impl.listener;

import com.mapr.fs.jni.NativeData;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class NativeDataParserTest {

    @Test
    public void testNextTopicPartition() {
        NativeData nativeData = new NativeData();
        nativeData.long_data = new long[]{2, 0, 2, 2, 0};
        nativeData.byte_data = new byte[]{47, 115, 116, 49};
        nativeData.err = 0;
        nativeData.longArrIndex = 0;
        nativeData.byteArrIndex = 0;
        NativeDataParser nativeDataParser = new NativeDataParser(nativeData);

        TopicPartition tp = nativeDataParser.getNextTopicPartition();
        assertEquals("/s:t1", tp.topic());
        assertEquals(0, tp.partition());
    }

    @Test
    public void testParseListenerRecords() {
        NativeData nativeData = new NativeData();
        nativeData.long_data = new long[]{2, 22, 2, 24, 0, 0, 1, 0, 1624557529564l, 2, 1, 0, 4, 1, 16, 5, 0};
        nativeData.byte_data = new byte[]{50, 116, 101, 115, 116, 99, 111, 110, 115,
                111, 108, 101, 45, 112, 114, 111, 100,
                117, 99, 101, 114, 0, 47, 115, 116, 50};
        nativeData.err = 0;
        nativeData.longArrIndex = 0;
        nativeData.byteArrIndex = 0;
        NativeDataParser nativeDataParser = new NativeDataParser(nativeData);

        Map<TopicPartition, List<ListenerRecord>> records = nativeDataParser.parseListenerRecords(false);
        Iterator iter = records.entrySet().iterator();
        assertEquals(true, iter.hasNext());
        Map.Entry entry = (Map.Entry) iter.next();
        assertEquals("/s:t2", ((TopicPartition) entry.getKey()).topic());
        assertEquals(0, ((TopicPartition) entry.getKey()).partition());
        assertEquals(1, ((List) entry.getValue()).size());
        assertEquals(false, iter.hasNext());
    }
}
