package com.mapr.kafka.eventstreams.impl.listener;

import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.jni.MarlinJniListener;
import com.mapr.fs.jni.NativeData;
import com.mapr.fs.proto.Dbserver;
import com.mapr.kafka.eventstreams.impl.tools.MockUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"jdk.internal.reflect.*", "javax.management.*", "javax.xml.*", "org.apache.xerces.*", "org.w3c.*", "org.apache.hadoop.fs.FileSystem$Cache$Key"})
@PrepareForTest({MapRFileSystem.class, MarlinJniListener.class, NativeDataParserV10.class, MarlinListenerImplV10.class})
@SuppressStaticInitializationFor({"org.apache.hadoop.conf.Configuration", "org.apache.hadoop.fs.FileSystem", "com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils"})
public class ListenerMerticsTest {
    private static Deserializer deserializer = new StringDeserializer();
    private static ConsumerConfig config = getConsumerConfig(deserializer);
    private static MBeanServer mBeanServer;
    private static ObjectName consumerFetchMetricsMBean;

    private static MarlinListenerImpl listener;

    private static String topic = "/s:t";
    private static String key = "recordKey";
    private static String value = "recordValue";
    private static String headerKey = "hKey";
    private static String headerValue = "hValue";
    private static List<ListenerRecord> recordsToFetch = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        consumerFetchMetricsMBean = new ObjectName("kafka.consumer:client-id=consumer--1,type=consumer-fetch-manager-metrics");

        MockUtil.mockMapRFileSystem();
        MockUtil.mockListenerNativeMethods();

        Map<TopicPartition, List<ListenerRecord>> records = new HashMap<>();
        records.put(new TopicPartition(topic, 0), recordsToFetch);

        NativeDataParserV10 nativeDataParser = mock(NativeDataParserV10.class);
        EasyMock.expect(nativeDataParser.parseListenerRecords(anyBoolean())).andReturn(records).anyTimes();
        PowerMock.expectNew(NativeDataParserV10.class, new Class[]{NativeData.class}, anyObject(NativeData.class))
                .andReturn(nativeDataParser).anyTimes();
        PowerMock.replay(nativeDataParser);
        PowerMock.replayAll();

        listener = new MarlinListenerImplV10(config, null, Dbserver.CDCOpenFormatType.COFT_NONE);
    }

    @Test
    public void testFetchMetrics() throws Exception {
        Headers headers = new RecordHeaders(new Header[]
                {new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))});
        ListenerRecord record1 = new ListenerRecord(topic, -1, 0, 0, 0,
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), headers, "p");
        ListenerRecord record2 = new ListenerRecord(topic, -1, 0, 0, 0,
                null, null, new RecordHeaders(), "p");
        int recordSize1 = estimatedRecordSize(record1);
        int recordSize2 = estimatedRecordSize(record2);

        pollRecord(record1);
        pollRecord(record2);

        double fetchSizeAvg = getJmxMetric("fetch-size-avg");
        double fetchSizeMax = getJmxMetric("fetch-size-max");
        double bytesConsumedRate = getJmxMetric("bytes-consumed-rate");
        double bytesConsumedTotal = getJmxMetric("bytes-consumed-total");
        double recordsConsumedRate = getJmxMetric("records-consumed-rate");
        double recordsConsumedTotal = getJmxMetric("records-consumed-total");

        assertEquals((recordSize1 + recordSize2) / 2, fetchSizeAvg, 0.1);
        assertEquals(recordSize1, fetchSizeMax, 0.1);
        assertEquals(recordSize1 + recordSize2, bytesConsumedTotal, 0.1);
        assertEquals(2, recordsConsumedTotal, 0.1);
        assertNotEquals(0, bytesConsumedRate);
        assertNotEquals(0, recordsConsumedRate);
    }

    private void pollRecord(ListenerRecord record) {
        recordsToFetch.add(record);
        listener.poll(100);
        recordsToFetch.remove(record);
    }

    private int estimatedRecordSize(ListenerRecord record) {
        int topicSize = record.topic() == null ? 0 : record.topic().length();
        int key = record.key() == null ? 0 : record.key().length;
        int value = record.value() == null ? 0 : record.value().length;
        int headersSize = Arrays.stream(record.headers().toArray())
                .mapToInt(h -> h.key().length() + h.value().length).sum();

        return topicSize + key + value + headersSize;
    }

    private static ConsumerConfig getConsumerConfig(Deserializer deserializer) {
        String keyDeserializerClassName = deserializer.getClass().getName();
        String valueDeserializerClassName = deserializer.getClass().getName();
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        props.setProperty(ConsumerConfig.METRICS_ENABLED_CONFIG, Boolean.toString(true));
        return new ConsumerConfig(props);
    }

    private static double getJmxMetric(String metric) throws Exception {
        return (double) mBeanServer.getAttribute(consumerFetchMetricsMBean, metric);
    }
}
