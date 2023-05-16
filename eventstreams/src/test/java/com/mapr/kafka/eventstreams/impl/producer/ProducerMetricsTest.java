package com.mapr.kafka.eventstreams.impl.producer;

import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.ShimLoader;
import com.mapr.fs.jni.MarlinJniListener;
import com.mapr.fs.jni.MarlinJniProducer;
import com.mapr.fs.jni.MarlinProducerResult;
import com.mapr.kafka.eventstreams.impl.listener.NativeDataParserV10;
import com.mapr.kafka.eventstreams.impl.tools.MockUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.BeforeClass;
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
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"jdk.internal.reflect.*", "javax.management.*", "javax.xml.*", "org.apache.xerces.*", "org.w3c.*", "org.apache.hadoop.fs.FileSystem$Cache$Key"})
@PrepareForTest({MapRFileSystem.class, MarlinJniProducer.class, NativeDataParserV10.class})
@SuppressStaticInitializationFor({"org.apache.hadoop.conf.Configuration", "org.apache.hadoop.fs.FileSystem", "com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils"})
public class ProducerMetricsTest {
    private static Serializer<byte[]> serializer = new ByteArraySerializer();
    private static ProducerConfig config = getProducerConfig(serializer);
    private static MarlinProducerImpl producer;
    private static MBeanServer mBeanServer;
    private static ObjectName producerMetricsMBean;
    private static ObjectName producerTopicMetricsMBean;

    private static String topic = "/s:t";
    private static String key = "recordKey";
    private static String value = "recordValue";
    private static String headerKey = "hKey";
    private static String headerValue = "hValue";

    @BeforeClass
    public static void staticSetUp(){
        //something going wrong when calling ShimLoader.load() so then we have ClassNotFound on one of inner classes
        //of MarlinJniListener. In order to prevent it just load it before like this
        MarlinJniListener.class.getDeclaredMethods();
    }

    @Before
    public void setUp() throws Exception {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        producerMetricsMBean = new ObjectName("kafka.producer:client-id=producer-1,type=producer-metrics");
        producerTopicMetricsMBean = new ObjectName("kafka.producer:client-id=producer-1,type=producer-topic-metrics,topic=\"" + topic + "\"");

        MockUtil.mockProducerNativeMethods();
        MockUtil.mockMapRFileSystem();

        PowerMock.suppress(ShimLoader.class.getMethod("load"));

        PowerMock.replayAll();

        producer = new MarlinProducerImplV10(config);
    }

    @Test
    public void testSendMetrics() throws Exception {
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic,
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        record1.headers().add(headerKey, headerValue.getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, null, null);
        int expectedRecordSize1 = topic.length() + key.length() + value.length() + headerKey.length() + headerValue.length();
        int expectedRecordSize2 = topic.length();

        send(record1);
        send(record2);
        producer.close(); //blocks until all records completes and all metrics are updated

        double recordSendRate = getJmxProducerMetric(producerMetricsMBean, "record-send-rate");
        double recordSendTotal = getJmxProducerMetric(producerMetricsMBean, "record-send-total");
        double recordSizeAvg = getJmxProducerMetric(producerMetricsMBean, "record-size-avg");
        double recordSizeMax = getJmxProducerMetric(producerMetricsMBean, "record-size-max");
        double byteRate = getJmxProducerMetric(producerTopicMetricsMBean, "byte-rate");
        double byteTotal = getJmxProducerMetric(producerTopicMetricsMBean, "byte-total");
        double recordSendRatePerTopic = getJmxProducerMetric(producerTopicMetricsMBean, "record-send-rate");
        double recordSendTotalPerTopic = getJmxProducerMetric(producerTopicMetricsMBean, "record-send-total");

        assertEquals((expectedRecordSize1 + expectedRecordSize2) / 2, recordSizeAvg, 0.1);
        assertEquals(expectedRecordSize1, recordSizeMax, 0.1);
        assertEquals(expectedRecordSize1 + expectedRecordSize2, byteTotal, 0.1);
        assertEquals(2, recordSendTotal, 0.1);
        assertEquals(2, recordSendTotalPerTopic, 0.1);
        assertNotEquals(0, recordSendRate);
        assertNotEquals(0, byteRate);
        assertNotEquals(0, recordSendRatePerTopic);
    }

    @Test
    public void testErrorMetrics() throws Exception {
        MarlinProducerResult[] results = new MarlinProducerResult[1];
        results[0] = new MarlinProducerResultImplV10(topic, -1, null, key.length(), value.length());

        // simulate permission denied error result
        producer.handleJniCallbacks(1, new long[0], new long[0], results, -1, 13);

        double recordErrorRate = getJmxProducerMetric(producerMetricsMBean, "record-error-rate");
        double recordErrorTotal = getJmxProducerMetric(producerMetricsMBean, "record-error-total");
        double recordErrorRatePerTopic = getJmxProducerMetric(producerTopicMetricsMBean, "record-error-rate");
        double recordErrorTotalPerTopic = getJmxProducerMetric(producerTopicMetricsMBean, "record-error-total");

        assertEquals(1, recordErrorTotal, 0.1);
        assertEquals(1, recordErrorTotalPerTopic, 0.1);
        assertNotEquals(0, recordErrorRate);
        assertNotEquals(0, recordErrorRatePerTopic);
    }

    private static ProducerConfig getProducerConfig(Serializer serializer) {
        String serializerClassName = serializer.getClass().getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClassName);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClassName);
        props.setProperty(ProducerConfig.METRICS_ENABLED_CONFIG, Boolean.toString(true));
        return new ProducerConfig(props);
    }

    private void send(ProducerRecord<byte[], byte[]> record) {
        byte[] key = serializer.serialize(record.topic(), record.key());
        byte[] value = serializer.serialize(record.topic(), record.value());
        MarlinProducerResult[] results = new MarlinProducerResult[1];
        results[0] = new MarlinProducerResultImplV10(topic, -1, null,
                key != null ? key.length : 0, value != null ? value.length : 0);
        producer.send(record, -1, key, value, null);
        producer.handleJniCallbacks(1, new long[0], new long[0], results, -1, 0);
    }

    private double getJmxProducerMetric(ObjectName bean, String metric) throws Exception {
        return (double) mBeanServer.getAttribute(bean, metric);
    }

}
