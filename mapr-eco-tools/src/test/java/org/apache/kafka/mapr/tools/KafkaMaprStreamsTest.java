package org.apache.kafka.mapr.tools;

import com.mapr.kafka.eventstreams.Admin;
import com.mapr.kafka.eventstreams.StreamDescriptor;
import org.apache.kafka.common.KafkaException;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaMaprStreamsTest extends EasyMockSupport {
    private static final String STREAM = "/stream";
    private Admin maprStreamsAdmin;
    private KafkaMaprStreams kafkaMaprStreams;

    @BeforeClass
    public static void avoidShimLoading() throws Exception {
        SuppressionUtil.suppressShimLoader();
    }

    @Before
    public void setUp() {
        maprStreamsAdmin = mock(Admin.class);
        kafkaMaprStreams = new KafkaMaprStreams(maprStreamsAdmin);
    }

    @Test
    public void checkIfStreamsExists() throws IOException {
        expect(maprStreamsAdmin.streamExists(STREAM)).andReturn(true);
        replayAll();

        assertTrue(kafkaMaprStreams.streamExists(STREAM));

        verifyAll();
    }

    @Test
    public void rethrowsKafkaExpectionOnStreamExists() throws IOException {
        expect(maprStreamsAdmin.streamExists(STREAM)).andThrow(new IOException());
        replayAll();

        try {
            assertTrue(kafkaMaprStreams.streamExists(STREAM));
            fail("Expected KafkaException");
        } catch (KafkaException expected) {
        }

        verifyAll();
    }

    @Test
    public void preservesEnabledStreamLogCompaction() throws IOException {
        StreamDescriptor descriptor = mock(StreamDescriptor.class);
        expect(descriptor.getCompact()).andReturn(true);
        expect(maprStreamsAdmin.getStreamDescriptor(STREAM)).andReturn(descriptor);
        replayAll();

        kafkaMaprStreams.ensureStreamLogCompactionIsEnabled(STREAM);

        verifyAll();
    }

    @Test
    public void enablesStreamLogCompactionAndZeroTTL() throws IOException {
        StreamDescriptor descriptor = mock(StreamDescriptor.class);
        expect(descriptor.getCompact()).andReturn(false);
        expect(maprStreamsAdmin.getStreamDescriptor(STREAM)).andReturn(descriptor);
        descriptor.setCompact(true);
        descriptor.setTimeToLiveSec(0);
        maprStreamsAdmin.editStream(STREAM, descriptor);
        replayAll();

        kafkaMaprStreams.ensureStreamLogCompactionIsEnabled(STREAM);

        verifyAll();
    }

    @Test
    public void rethrowsIOExceptionAsKafkaForEnsureStreamLogCompactionIsEnabled() throws IOException {
        expect(maprStreamsAdmin.getStreamDescriptor(STREAM)).andThrow(new IOException());
        replayAll();

        try {
            kafkaMaprStreams.ensureStreamLogCompactionIsEnabled(STREAM);
            fail("Expected KafkaException");
        } catch (KafkaException expected) {
        }

        verifyAll();
    }

    @Test
    public void closesAdmin() {
        maprStreamsAdmin.close();
        replayAll();

        kafkaMaprStreams.close();

        verifyAll();
    }

    @Test
    public void getsShortTopicNameFromFullTopicName() {
        String fromFullName = KafkaMaprStreams.getShortTopicNameFromFullTopicName("/ss:topic");
        String fromShortName = KafkaMaprStreams.getShortTopicNameFromFullTopicName("topic");

        assertEquals("topic", fromFullName);
        assertEquals("topic", fromShortName);
    }

    @Test
    public void createsStreamForClusterAdmin() throws IOException {
        Capture<StreamDescriptor> captured = newCapture();
        maprStreamsAdmin.createStream(eq(STREAM), capture(captured));
        replayAll();

        kafkaMaprStreams.createStreamForClusterAdmin(STREAM);
        verifyAll();

        StreamDescriptor value = captured.getValue();
        assertNull(value.getProducePerms());
        assertNull(value.getConsumePerms());
    }

    @Test
    public void createsStreamForAllUsers() throws IOException {
        Capture<StreamDescriptor> captured = newCapture();
        maprStreamsAdmin.createStream(eq(STREAM), capture(captured));
        replayAll();

        kafkaMaprStreams.createStreamForAllUsers(STREAM);
        verifyAll();

        StreamDescriptor value = captured.getValue();
        assertEquals("p", value.getProducePerms());
        assertEquals("p", value.getConsumePerms());
    }

    @Test
    public void createsStreamForCurrentUserWhichIsNotClusterAdmin() throws IOException {
        KafkaMaprTools.tools = mock(KafkaMaprTools.class);
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn("mapruser1");
        expect(KafkaMaprTools.tools().getClusterAdminUserName()).andReturn("mapr");
        Capture<StreamDescriptor> captured = newCapture();
        maprStreamsAdmin.createStream(eq(STREAM), capture(captured));
        replayAll();

        kafkaMaprStreams.createStreamForCurrentUser(STREAM);
        verifyAll();

        StreamDescriptor value = captured.getValue();
        assertEquals("u:mapr | u:mapruser1", value.getProducePerms());
        assertEquals("u:mapr | u:mapruser1", value.getConsumePerms());
    }

    @Test
    public void createsStreamForCurrentUserWhichIsClusterAdmin() throws IOException {
        KafkaMaprTools.tools = mock(KafkaMaprTools.class);
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn("mapr");
        expect(KafkaMaprTools.tools().getClusterAdminUserName()).andReturn("mapr");
        Capture<StreamDescriptor> captured = newCapture();
        maprStreamsAdmin.createStream(eq(STREAM), capture(captured));
        replayAll();

        kafkaMaprStreams.createStreamForCurrentUser(STREAM);
        verifyAll();

        StreamDescriptor value = captured.getValue();
        assertNull(value.getProducePerms());
        assertNull(value.getConsumePerms());
    }

    @Test
    public void suppressExceptionOnConcurrentlyCreatedStream() throws IOException {
        maprStreamsAdmin.createStream(eq(STREAM), anyObject());
        expectLastCall().andThrow(new IOException());
        expect(maprStreamsAdmin.streamExists(STREAM)).andReturn(true);
        replayAll();

        kafkaMaprStreams.createStreamForClusterAdmin(STREAM);
        verifyAll();
    }

    @Test
    public void rethrowExceptionOnFailedStreamCreation() throws IOException {
        maprStreamsAdmin.createStream(eq(STREAM), anyObject());
        expectLastCall().andThrow(new IOException());
        expect(maprStreamsAdmin.streamExists(STREAM)).andReturn(false);
        replayAll();

        try {
            kafkaMaprStreams.createStreamForClusterAdmin(STREAM);
            fail("Expected KafkaException");
        } catch (KafkaException expected) {
        }
        verifyAll();
    }
}