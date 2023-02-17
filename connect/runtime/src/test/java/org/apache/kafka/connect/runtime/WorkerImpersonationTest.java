package org.apache.kafka.connect.runtime;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest.NOOP_OPERATOR;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Worker.class, Plugins.class, ConnectUtils.class, UserGroupInformation.class})
@PowerMockIgnore({"javax.management.*", "javax.xml.*", "org.apache.xerces.*", "org.w3c.*", "javax.security.*"})
@SuppressStaticInitializationFor("com.mapr.baseutils.JVMProperties")
public class WorkerImpersonationTest extends WorkerTest {
    @Override
    public void setup() {
        super.setup();
        workerProps.put(WorkerConfig.AUTHENTICATION_ENABLE_CONFIG, Boolean.toString(true));
        workerProps.put(WorkerConfig.ENABLE_IMPERSONATION_CONFIG, Boolean.toString(true));
        config = new StandaloneConfig(workerProps);
    }

    @Test
    public void testImpersonationWhenInitializingKafkaProducer() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();
        expectImpersonation();
        expectStartTask(workerTask, task, TASK_ID);

        UserGroupInformation[] producerInitUser = new UserGroupInformation[1];
        expectNewKafkaProducer(producerInitUser);

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, task.getClass().getName());
        origProps.put(TaskConfig.TASK_USER_CONFIG, TASK_USER);
        boolean taskStarted = worker.startTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertTrue(taskStarted);

        assertEquals(TASK_USER, producerInitUser[0].getShortUserName());
    }

    @Test
    public void testImpersonationWhenInitializingKafkaConsumer() throws Exception {
        WorkerSinkTask sinkTask = mock(WorkerSinkTask.class);
        TestSinkTask task = mock(TestSinkTask.class);
        ConnectorTaskId taskId = new ConnectorTaskId("job", 1);

        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();
        expectImpersonation();
        expectStartTask(sinkTask, task, taskId);

        UserGroupInformation[] consumerInitUser = new UserGroupInformation[1];
        expectNewKafkaConsumer(consumerInitUser);

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, task.getClass().getName());
        origProps.put(TaskConfig.TASK_USER_CONFIG, TASK_USER);
        boolean taskStarted = worker.startTask(taskId, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertTrue(taskStarted);

        assertEquals(TASK_USER, consumerInitUser[0].getShortUserName());
    }

    private void expectStartTask(WorkerTask returned, Task task, ConnectorTaskId taskId) throws Exception {
        EasyMock.expect(returned.id()).andStubReturn(taskId);
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        if (returned instanceof WorkerSourceTask) {
            expectNewWorkerTask();
        } else if (returned instanceof WorkerSinkTask)
            expectNewWorkerSinkTask((WorkerSinkTask) returned, (TestSinkTask) task, taskId);
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, task.getClass().getName());
        origProps.put(TaskConfig.TASK_USER_CONFIG, TASK_USER);
        TaskConfig taskConfig = new TaskConfig(origProps);
        EasyMock.expect(plugins.newTask(task.getClass())).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");
        returned.initialize(taskConfig);
        EasyMock.expectLastCall();
        expectTaskKeyConverters(Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER, taskKeyConverter);
        expectTaskValueConverters(Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER, taskValueConverter);
        expectTaskHeaderConverter(Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);
        EasyMock.expect(executorService.submit(returned)).andReturn(null);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestConnector.class.getName())).andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader).times(2);
        EasyMock.expect(returned.loader()).andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader).times(2);
        plugins.connectorClass(WorkerTestConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestConnector.class);
        returned.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(returned.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        EasyMock.expectLastCall();
        returned.removeMetrics();
        EasyMock.expectLastCall();
        expectStopStorage();
        expectClusterId();
    }

    private void expectNewKafkaProducer(UserGroupInformation[] user) throws Exception {
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        PowerMock.expectNew(KafkaProducer.class, new Class[]{Map.class}, anyObject(Map.class))
                .andAnswer(() -> {
                    user[0] = UserGroupInformation.getCurrentUser();
                    return producer;
                }).once();
    }

    private void expectNewKafkaConsumer(UserGroupInformation[] user) throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        PowerMock.expectNew(KafkaConsumer.class, new Class[]{Map.class}, anyObject(Map.class))
                .andAnswer(() -> {
                    user[0] = UserGroupInformation.getCurrentUser();
                    return consumer;
                }).once();
    }

    private void expectNewWorkerSinkTask(WorkerSinkTask returned, TestSinkTask task, ConnectorTaskId taskId) throws Exception {
        PowerMock.expectNew(
                        WorkerSinkTask.class,
                        EasyMock.eq(taskId),
                        EasyMock.eq(task),
                        anyObject(TaskStatus.Listener.class),
                        EasyMock.eq(TargetState.STARTED),
                        EasyMock.eq(config),
                        anyObject(ClusterConfigState.class),
                        anyObject(ConnectMetrics.class),
                        anyObject(Converter.class),
                        anyObject(Converter.class),
                        anyObject(HeaderConverter.class),
                        EasyMock.eq(new TransformationChain<>(Collections.emptyList(), NOOP_OPERATOR)),
                        anyObject(KafkaConsumer.class),
                        EasyMock.eq(pluginLoader),
                        anyObject(Time.class),
                        anyObject(RetryWithToleranceOperator.class),
                        anyObject(WorkerErrantRecordReporter.class),
                        anyObject(StatusBackingStore.class))
                .andReturn(returned);
    }

    private static class TestSinkTask extends SinkTask {
        public TestSinkTask() {
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public void put(Collection<SinkRecord> records) {
        }

        @Override
        public void stop() {
        }
    }
}
