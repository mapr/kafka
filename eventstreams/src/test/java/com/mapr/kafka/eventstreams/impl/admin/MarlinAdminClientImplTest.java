package com.mapr.kafka.eventstreams.impl.admin;

import com.mapr.fs.jni.MarlinJniListener;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class MarlinAdminClientImplTest {
    private static final String DEFAULT_STREAM = "/default-stream";

    private String existingStream = "/existing-stream";
    private String nonExistingStream = "/non-existing-stream";

    private String testTopic1 = "testTopic1";
    private String testTopic2 = "testTopic2";
    private String testTopic3 = "testTopic3";
    private String testTopic4 = "testTopic4";

    private String fullTestTopic1 = "/default-stream:testTopic1";
    private String fullTestTopic2 = "/existing-stream:testTopic2";
    private String fullTestTopic3 = "/non-existing-stream:testTopic3";
    private String fullTestTopic4 = "/existing-stream:testTopic4";

    private NewTopic newTestTopic1 = new NewTopic(testTopic1, 1, (short) 0);
    private NewTopic newTestTopic2 = new NewTopic(fullTestTopic2, 2, (short) 0);
    private NewTopic newTestTopic3 = new NewTopic(fullTestTopic3, 3, (short) 0);

    private TopicPartition testTopic1Part0 = new TopicPartition(testTopic1, 0);
    private TopicPartition testTopic2Part0 = new TopicPartition(fullTestTopic2, 0);
    private TopicPartition testTopic2Part1 = new TopicPartition(fullTestTopic2, 1);
    private TopicPartition testTopic2Part2 = new TopicPartition(fullTestTopic2, 2);
    private TopicPartition testTopic3Part0 = new TopicPartition(fullTestTopic3, 0);
    private TopicPartition testTopic3Part1 = new TopicPartition(fullTestTopic3, 1);
    private TopicPartition testTopic4Part0 = new TopicPartition(fullTestTopic4, 0);

    private Map<TopicPartition, OffsetSpec> testTopic1PartOffsets = Map.of(testTopic1Part0, new OffsetSpec());
    private Map<TopicPartition, OffsetSpec> testTopic2PartOffsets = Map.of(testTopic2Part0, new OffsetSpec(), testTopic2Part1, new OffsetSpec());
    private Map<TopicPartition, OffsetSpec> testTopic2PartOffsetsWrongPart = Map.of(testTopic2Part0, new OffsetSpec(), testTopic2Part2, new OffsetSpec());
    private Map<TopicPartition, OffsetSpec> testTopic3PartOffsets = Map.of(testTopic3Part0, new OffsetSpec(), testTopic3Part1, new OffsetSpec());
    private Map<TopicPartition, OffsetSpec> testTopic4PartOffsets = Map.of(testTopic4Part0, new OffsetSpec());

    private MockAdmin mockAdmin;
    private MarlinAdminClientImpl adminClientWithDefaultStream;
    private MarlinAdminClientImpl adminClientWithoutDefaultStream;

    @BeforeClass
    public static void staticSetUp(){
        //something going wrong when calling ShimLoader.load() so then we have ClassNotFound on one of inner classes
        //of MarlinJniListener. In order to prevent it just load it before like this
        MarlinJniListener.class.getDeclaredMethods();
    }

    @Before
    public void setUp() {
        this.mockAdmin = new MockAdmin();
        this.adminClientWithDefaultStream = new MarlinAdminClientImpl(mockAdmin, DEFAULT_STREAM);
        this.adminClientWithoutDefaultStream = new MarlinAdminClientImpl(mockAdmin, null);
    }

    @Test
    public void testTopicCreationWithDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(List.of(DEFAULT_STREAM, existingStream));

        Future<Void> createTopic = adminClientWithDefaultStream.createTopics(singletonList(newTestTopic1)).all();
        createTopic.get();

        Future<Void> createTopicWithStream = adminClientWithDefaultStream.createTopics(singletonList(newTestTopic2)).all();
        createTopicWithStream.get();

        try {
            adminClientWithDefaultStream.createTopics(singletonList(newTestTopic2)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(TopicExistsException.class, e.getCause().getClass());
        }

        try {
            adminClientWithDefaultStream.createTopics(singletonList(newTestTopic3)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testTopicCreationWithoutDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(List.of(DEFAULT_STREAM, existingStream));

        try {
            adminClientWithoutDefaultStream.createTopics(singletonList(newTestTopic1)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(KafkaException.class, e.getCause().getClass());
        }

        Future<Void> createTopicWithStream = adminClientWithoutDefaultStream.createTopics(singletonList(newTestTopic2)).all();
        createTopicWithStream.get();

        try {
            adminClientWithoutDefaultStream.createTopics(singletonList(newTestTopic2)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(TopicExistsException.class, e.getCause().getClass());
        }

        try {
            adminClientWithoutDefaultStream.createTopics(singletonList(newTestTopic3)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testDeleteTopicsWithDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(List.of(fullTestTopic1, fullTestTopic2));

        Future<Void> deleteTopic = adminClientWithDefaultStream.deleteTopics(singletonList(testTopic1)).all();
        deleteTopic.get();

        Future<Void> deleteTopicWithStream = adminClientWithDefaultStream.deleteTopics(singletonList(fullTestTopic2)).all();
        deleteTopicWithStream.get();

        try {
            adminClientWithDefaultStream.deleteTopics(singletonList(fullTestTopic3)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }

        try {
            adminClientWithDefaultStream.deleteTopics(singletonList(fullTestTopic4)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic /existing-stream:testTopic4 does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testDeleteTopicsWithoutDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(List.of(fullTestTopic1, fullTestTopic2));

        try {
            adminClientWithoutDefaultStream.deleteTopics(singletonList(testTopic1)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(KafkaException.class, e.getCause().getClass());
        }

        Future<Void> deleteTopicWithStream = adminClientWithoutDefaultStream.deleteTopics(singletonList(fullTestTopic2)).all();
        deleteTopicWithStream.get();

        try {
            adminClientWithoutDefaultStream.deleteTopics(singletonList(fullTestTopic3)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }

        try {
            adminClientWithoutDefaultStream.deleteTopics(singletonList(fullTestTopic4)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic /existing-stream:testTopic4 does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testListTopicsWithDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(List.of(fullTestTopic1, fullTestTopic2, fullTestTopic4));
        List<String> topicsFromDefault = List.of(testTopic1);
        List<String> topicsFromExistingStream = List.of(testTopic2, testTopic4);

        KafkaFuture<Collection<TopicListing>> topicListings = adminClientWithDefaultStream.listTopics().listings();
        List<String> topics = topicListings.get().stream().map(TopicListing::name).collect(Collectors.toList());
        assertEquals(topicsFromDefault, topics);

        KafkaFuture<Collection<TopicListing>> topicListingsWithStream = adminClientWithDefaultStream.listTopics(existingStream).listings();
        topics = topicListingsWithStream.get().stream().map(TopicListing::name).collect(Collectors.toList());
        assertEquals(topicsFromExistingStream, topics);

        try {
            adminClientWithDefaultStream.listTopics(nonExistingStream).listings().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testListTopicsWithoutDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(List.of(fullTestTopic1, fullTestTopic2, fullTestTopic4));
        List<String> topicsFromExistingStream = List.of(testTopic2, testTopic4);

        try {
            adminClientWithoutDefaultStream.listTopics().listings().get();
            fail();
        } catch (Exception e) {
            assertEquals(KafkaException.class, e.getClass());
        }

        KafkaFuture<Collection<TopicListing>> topicListingsWithStream = adminClientWithoutDefaultStream.listTopics(existingStream).listings();
        List<String> topics = topicListingsWithStream.get().stream().map(TopicListing::name).collect(Collectors.toList());
        assertEquals(topicsFromExistingStream, topics);

        try {
            adminClientWithoutDefaultStream.listTopics(nonExistingStream).listings().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testDescribeTopicsWithDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(Map.of(fullTestTopic1, 1, fullTestTopic2, 2));

        KafkaFuture<Map<String, TopicDescription>> topicDesc = adminClientWithDefaultStream.describeTopics(singletonList(testTopic1)).all();
        TopicDescription desc = topicDesc.get().get(fullTestTopic1);
        assertEquals(1, desc.partitions().size());

        KafkaFuture<Map<String, TopicDescription>> topicDescWithStream = adminClientWithDefaultStream.describeTopics(singletonList(fullTestTopic2)).all();
        desc = topicDescWithStream.get().get(fullTestTopic2);
        assertEquals(2, desc.partitions().size());

        try {
            adminClientWithoutDefaultStream.describeTopics(singletonList(fullTestTopic3)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }

        try {
            adminClientWithoutDefaultStream.describeTopics(singletonList(fullTestTopic4)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic /existing-stream:testTopic4 does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testDescribeTopicsWithoutDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(Map.of(fullTestTopic1, 1, fullTestTopic2, 2));

        try {
            adminClientWithoutDefaultStream.describeTopics(singletonList(testTopic1)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(KafkaException.class, e.getCause().getClass());
        }

        KafkaFuture<Map<String, TopicDescription>> topicDescWithStream = adminClientWithDefaultStream.describeTopics(singletonList(fullTestTopic2)).all();
        TopicDescription desc = topicDescWithStream.get().get(fullTestTopic2);
        assertEquals(2, desc.partitions().size());

        try {
            adminClientWithoutDefaultStream.describeTopics(singletonList(fullTestTopic3)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }

        try {
            adminClientWithoutDefaultStream.describeTopics(singletonList(fullTestTopic4)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic /existing-stream:testTopic4 does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testCreatePartitionsWithDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(Map.of(fullTestTopic1, 1, fullTestTopic2, 2));
        NewPartitions inc = NewPartitions.increaseTo(5);

        KafkaFuture<Void> createPartitions = adminClientWithDefaultStream.createPartitions(Map.of(testTopic1, inc)).all();
        createPartitions.get();

        createPartitions = adminClientWithDefaultStream.createPartitions(Map.of(fullTestTopic2, inc)).all();
        createPartitions.get();

        try {
            adminClientWithDefaultStream.createPartitions(Map.of(fullTestTopic3, inc)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }

        try {
            adminClientWithDefaultStream.createPartitions(Map.of(fullTestTopic4, inc)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic /existing-stream:testTopic4 does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testCreatePartitionsWithoutDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(Map.of(fullTestTopic1, 1, fullTestTopic2, 2));
        NewPartitions inc = NewPartitions.increaseTo(5);

        try {
            adminClientWithoutDefaultStream.createPartitions(Map.of(testTopic1, inc)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(KafkaException.class, e.getCause().getClass());
        }

        KafkaFuture<Void> createPartitions = adminClientWithoutDefaultStream.createPartitions(Map.of(fullTestTopic2, inc)).all();
        createPartitions.get();

        try {
            adminClientWithoutDefaultStream.createPartitions(Map.of(fullTestTopic3, inc)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }

        try {
            adminClientWithoutDefaultStream.createPartitions(Map.of(fullTestTopic4, inc)).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic /existing-stream:testTopic4 does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testListOffsetsWithDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(Map.of(fullTestTopic1, 1, fullTestTopic2, 2));
        NewPartitions inc = NewPartitions.increaseTo(5);

        KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> offsetsResult
                = adminClientWithDefaultStream.listOffsets(testTopic1PartOffsets).all();
        offsetsResult.get();

        offsetsResult = adminClientWithDefaultStream.listOffsets(testTopic2PartOffsets).all();
        offsetsResult.get();

        try {
            adminClientWithDefaultStream.listOffsets(testTopic2PartOffsetsWrongPart).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Partition 2 cannot be found in the topic /existing-stream:testTopic2",
                    e.getCause().getMessage());
        }

        try {
            adminClientWithDefaultStream.listOffsets(testTopic4PartOffsets).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic testTopic4 cannot be found in the stream /existing-stream",
                    e.getCause().getMessage());
        }

        try {
            adminClientWithDefaultStream.listOffsets(testTopic3PartOffsets).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }
    }

    @Test
    public void testListOffsetsWithoutDefaultStream() throws Exception {
        mockAdmin.setExistingStreamsAndTopics(Map.of(fullTestTopic1, 1, fullTestTopic2, 2));
        NewPartitions inc = NewPartitions.increaseTo(5);

        try {
            adminClientWithoutDefaultStream.listOffsets(testTopic1PartOffsets).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(KafkaException.class, e.getCause().getClass());
        }

        KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> offsetsResult
                = adminClientWithoutDefaultStream.listOffsets(testTopic2PartOffsets).all();
        offsetsResult.get();

        try {
            adminClientWithoutDefaultStream.listOffsets(testTopic2PartOffsetsWrongPart).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Partition 2 cannot be found in the topic /existing-stream:testTopic2",
                    e.getCause().getMessage());
        }

        try {
            adminClientWithoutDefaultStream.listOffsets(testTopic4PartOffsets).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Topic testTopic4 cannot be found in the stream /existing-stream",
                    e.getCause().getMessage());
        }

        try {
            adminClientWithoutDefaultStream.listOffsets(testTopic3PartOffsets).all().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(UnknownTopicOrPartitionException.class, e.getCause().getClass());
            assertEquals("Stream /non-existing-stream does not exist.", e.getCause().getMessage());
        }
    }
}
