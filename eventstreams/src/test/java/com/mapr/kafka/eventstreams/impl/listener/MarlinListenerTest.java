package com.mapr.kafka.eventstreams.impl.listener;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(EasyMockRunner.class)
public class MarlinListenerTest {

    @Test
    public void testPoll() {
        byte[] key = {0, 0, 0, -56};
        byte[] next = {110, 101, 120, 116};
        List<ListenerRecord> listenerRecords = new ArrayList<>();
        listenerRecords.add(new ListenerRecord("/s:t", 1, 0, 0,
                0, key, next, null, "producer"));
        listenerRecords.add(new ListenerRecord("/s:t", 1, 1, 0,
                0, key, next, null, "producer"));
        listenerRecords.add(new ListenerRecord("/s:t", 1, 2, 0,
                0, key, next, null, "producer"));
        TopicPartition topicPartition = new TopicPartition("/s:t", 0);
        Map<TopicPartition, List<ListenerRecord>> marlinRecMap = new HashMap<>();
        marlinRecMap.put(topicPartition, listenerRecords);

        IntegerDeserializer intDeserializer = new IntegerDeserializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        MarlinListenerImpl listenerImpl = EasyMock.createNiceMock(MarlinListenerImpl.class);
        MarlinListener marlinListener = new MarlinListener(getConsumerConfig(intDeserializer, stringDeserializer),
                intDeserializer, stringDeserializer, listenerImpl);
        EasyMock.expect(listenerImpl.poll(100)).andReturn(marlinRecMap).once();
        EasyMock.replay(listenerImpl);

        ConsumerRecords records = marlinListener.poll(100);
        Iterator<ConsumerRecord<Integer, String>> iter = records.iterator();
        int c = 0;
        for (; iter.hasNext(); c++) {
            ConsumerRecord<Integer, String> record = iter.next();
            assertEquals(1, record.partition());
            assertEquals("/s:t", record.topic());
            assertEquals(Optional.of(200), Optional.of(record.key()));
            assertEquals("next", record.value());
            assertEquals(c, record.offset());
        }
        assertEquals(3, c);

        EasyMock.verify(listenerImpl);
    }

    @Test
    public void testToKafkaConsumerRecordWithIntegerAndStringDeserializer() {
        IntegerDeserializer intDeserializer = new IntegerDeserializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        MarlinListenerImpl listenerImpl = EasyMock.createNiceMock(MarlinListenerImpl.class);
        MarlinListener marlinListener = new MarlinListener(getConsumerConfig(intDeserializer, stringDeserializer),
                intDeserializer, stringDeserializer, listenerImpl);
        byte[] key1 = {0, 0, 0, -56};
        byte[] second = {115, 101, 99, 111, 110, 100};
        ListenerRecord record2 = new ListenerRecord("/s:t1", 0, 1, 0,
                0, key1, second, null, "producer");

        ConsumerRecord<String, String> consumerRecord =
                marlinListener.toKafkaConsumerRecord(record2, intDeserializer, stringDeserializer);
        assertEquals(200, consumerRecord.key());
        assertEquals("second", consumerRecord.value());
    }

    private ConsumerConfig getConsumerConfig(Deserializer keyDeserializer, Deserializer valueDeserializer) {
        String keyDeserializerClassName = keyDeserializer.getClass().getName();
        String valueDeserializerClassName = valueDeserializer.getClass().getName();
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        return new ConsumerConfig(props);
    }
}
