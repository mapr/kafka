package com.mapr.kafka.eventstreams.impl.tools;

import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.jni.MapRUserInfo;
import com.mapr.fs.jni.MarlinJniListener;
import com.mapr.fs.jni.MarlinJniProducer;
import com.mapr.fs.jni.MarlinProducerResult;
import com.mapr.fs.jni.NativeData;
import org.apache.hadoop.conf.Configuration;
import org.powermock.api.easymock.PowerMock;

import java.lang.reflect.Method;

import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

public class MockUtil {
    //mocks only static initialization and CurrentUserInfo
    public static void mockMapRFileSystem() throws Exception {
        suppress(method(MapRFileSystem.class, "CurrentUserInfo"));
        String cacheClassName = "org.apache.hadoop.fs.FileSystem$Cache";
        Configuration configurationMock = mock(Configuration.class);
        replay(configurationMock);
        Object cacheMock = mock(Class.forName(cacheClassName));
        replay(cacheMock);
        PowerMock.expectNew(cacheClassName, configurationMock)
                .andReturn(cacheMock);
    }

    //mocks all producer native methods to behave as they are executed successfully
    public static void mockProducerNativeMethods() throws Exception {
        Class clazz = MarlinJniProducer.class;

        Method openProducer = clazz.getDeclaredMethod("OpenProducer",
                String.class, int.class, boolean.class, boolean.class, boolean.class, long.class, long.class,
                long.class, String.class, MapRUserInfo.class);
        Method getDefaultClusterPath = clazz.getDeclaredMethod("GetDefaultClusterPath", long.class);
        Method send = clazz.getDeclaredMethod("Send", long.class, long.class, byte[].class,
                int.class, int[].class, int[].class, long[].class, int[].class, MarlinProducerResult[].class, int.class);
        Method flush = clazz.getDeclaredMethod("Flush", long.class);
        Method getTopicInfo = clazz.getDeclaredMethod("GetTopicInfo", long.class, String.class);
        Method closeProducer = clazz.getDeclaredMethod("CloseProducer", long.class);

        PowerMock.replace(method(clazz, openProducer.getName())).with((proxy, method, args) -> 1l);
        PowerMock.replace(method(clazz, getDefaultClusterPath.getName())).with((proxy, method, args) -> "/some/path");
        PowerMock.replace(method(clazz, send.getName())).with((proxy, method, args) -> 0);
        PowerMock.replace(method(clazz, flush.getName())).with((proxy, method, args) -> 0);
        PowerMock.replace(method(clazz, getTopicInfo.getName())).with((proxy, method, args) -> 1);
        PowerMock.replace(method(clazz, closeProducer.getName())).with((proxy, method, args) -> null);

        PowerMock.replayAll();
    }

    //mocks only OpenListener and Poll method to behave as they are executed successfully
    public static void mockListenerNativeMethods() throws Exception {
        Class clazz = MarlinJniListener.class;
        Method poll = clazz.getDeclaredMethod("Poll", long.class, long.class, NativeData.class);
        Method openListener = clazz.getDeclaredMethod("OpenListener", String.class, String.class, int.class,
                boolean.class, boolean.class, long.class, MarlinJniListener.MarlinCommitCallbackWrapper.class, long.class,
                int.class, int.class, int.class, int.class, String.class, String.class, long.class, boolean.class,
                MapRUserInfo.class, int.class, int.class);

        PowerMock.replace(openListener).with(((proxy, method, args) -> 1l));
        PowerMock.replace(poll).with(((proxy, method, args) -> 0));

        PowerMock.replayAll();
    }
}
