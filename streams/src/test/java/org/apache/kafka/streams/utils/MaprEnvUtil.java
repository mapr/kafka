package org.apache.kafka.streams.utils;

import com.mapr.kafka.eventstreams.Admin;
import com.mapr.kafka.eventstreams.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.mapr.tools.KafkaMaprStreams;
import org.apache.kafka.mapr.tools.KafkaMaprTools;
import org.apache.kafka.mapr.tools.KafkaMaprfs;
import org.easymock.EasyMock;
import org.powermock.api.easymock.PowerMock;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.mock;

public class MaprEnvUtil {
    /*
    Mocks mapr environment for tests. Requires following annotations on test class:
        @SuppressStaticInitializationFor({"com.mapr.kafka.eventstreams.Streams","com.mapr.baseutils.JVMProperties"})
        @RunWith(PowerMockRunner.class)
        @PowerMockIgnore({"javax.management.*", "javax.xml.*", "jdk.xml.*", "org.apache.xerces.*", "org.w3c.*"})
        @PrepareForTest({KafkaMaprTools.class, Streams.class})
     */
    public static void setUp() throws Exception{
        KafkaMaprTools tools = mock(KafkaMaprTools.class);
        PowerMock.mockStatic(KafkaMaprTools.class);
        EasyMock.expect(KafkaMaprTools.tools()).andReturn(tools).anyTimes();

        KafkaMaprfs maprfs = mock(KafkaMaprfs.class);
        EasyMock.expect(maprfs.exists(anyString())).andReturn(true).anyTimes();
        EasyMock.expect(tools.maprfs()).andReturn(maprfs).anyTimes();
        maprfs.requireExisting(anyString());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(maprfs.isAccessibleAsDirectory(anyString())).andReturn(true).anyTimes();

        KafkaMaprStreams streams = mock(KafkaMaprStreams.class);
        EasyMock.expect(tools.streams()).andReturn(streams).anyTimes();
        EasyMock.expect(streams.streamExists(anyString())).andReturn(true).anyTimes();
        streams.ensureStreamLogCompactionIsEnabled(anyString());
        EasyMock.expectLastCall().anyTimes();
        streams.close();
        EasyMock.expectLastCall().anyTimes();

        PowerMock.mockStatic(Streams.class, Streams.class.getMethod("newAdmin", Configuration.class));
        Admin admin = mock(Admin.class);
        EasyMock.expect(Streams.newAdmin(anyObject())).andReturn(admin).anyTimes();

        PowerMock.replay(tools, maprfs, streams);
        PowerMock.replayAll();
    }
}
