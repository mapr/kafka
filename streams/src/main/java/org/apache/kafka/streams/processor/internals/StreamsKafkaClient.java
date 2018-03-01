/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.BrokerNotFoundException;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StreamsKafkaClient {

    private static final ConfigDef CONFIG = StreamsConfig.configDef()
            .withClientSslSupport()
            .withClientSaslSupport();

    public static class Config extends AbstractConfig {

        static Config fromStreamsConfig(StreamsConfig streamsConfig) {
            return new Config(streamsConfig.originals());
        }

        Config(Map<?, ?> originals) {
            super(CONFIG, originals, false);
        }

    }
    private final AdminClient adminClient;
    private final Config streamsConfig;
    private final Map<String, String> defaultTopicConfigs = new HashMap<>();


    StreamsKafkaClient(final Config streamsConfig) {
        this.streamsConfig = streamsConfig;
        Properties adminClientProps = new Properties();
        adminClientProps.put(AdminClientConfig.STREAMS_ADMIN_DEFAULT_STREAM_CONFIG,
                streamsConfig.getString(StreamsConfig.STREAMS_DEFAULT_STREAM_CONFIG));
        this.adminClient = AdminClient.create(adminClientProps);
        extractDefaultTopicConfigs(streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX));
    }

    private void extractDefaultTopicConfigs(final Map<String, Object> configs) {
        for (final Map.Entry<String, Object> entry : configs.entrySet()) {
            if (entry.getValue() != null) {
                defaultTopicConfigs.put(entry.getKey(), entry.getValue().toString());
            }
        }
    }


    public static StreamsKafkaClient create(final Config streamsConfig) {
        return new StreamsKafkaClient(streamsConfig);
    }

    public static StreamsKafkaClient create(final StreamsConfig streamsConfig) {
        return create(Config.fromStreamsConfig(streamsConfig));
    }

    public void close() throws IOException {
        adminClient.close();
    }

    /**
     * Create a set of new topics using batch request.
     */
    public void createTopics(final Map<InternalTopicConfig, Integer> topicsMap, final int replicationFactor,
                             final long windowChangeLogAdditionalRetention, final MetadataResponse metadata) {

        List<NewTopic> topicsList = new ArrayList<NewTopic>();
        for (Map.Entry<InternalTopicConfig, Integer> entry : topicsMap.entrySet()) {
            InternalTopicConfig internalTopicConfig = entry.getKey();
            Integer partitions = entry.getValue();
            final Properties topicProperties = internalTopicConfig.toProperties(windowChangeLogAdditionalRetention);
            final Map<String, String> topicConfig = new HashMap<>(defaultTopicConfigs);
            for (String key : topicProperties.stringPropertyNames()) {
                topicConfig.put(key, topicProperties.getProperty(key));
            }

            topicsList.add(new NewTopic(internalTopicConfig.name(), partitions, (short) replicationFactor));
        }

        final CreateTopicsResult createTopicsResult = adminClient.createTopics(topicsList);

        for (final Map.Entry<String, KafkaFuture<Void>> createTopicResult : createTopicsResult.values().entrySet()) {
          try {
            createTopicResult.getValue().get();
          } catch (final ExecutionException couldNotCreateTopic) {
            final Throwable cause = couldNotCreateTopic.getCause();
            final String topicName = createTopicResult.getKey();

            if (!(cause instanceof TopicExistsException)) {
              throw new StreamsException("Could not create topic: " + topicName + " due to " + couldNotCreateTopic.getMessage());
            }
          } catch (final InterruptedException fatalException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(fatalException);
          }
        }
    }

    private Set<String> filterUnexistingTopics(Set<String> topics) throws ExecutionException, InterruptedException {
        if(streamsConfig.getString(StreamsConfig.STREAMS_DEFAULT_INTERNAL_STREAM_CONFIG).isEmpty()){
            return topics;
        }
        final Map <String, Set<String>> streamTopicsMap = new HashMap<>();
        for(String topic : topics){
            int colonIndex = topic.indexOf(":");
            String streamName = topic.substring(0, colonIndex);
            String topicName = topic.substring(colonIndex + 1);
            Set<String> tps = streamTopicsMap.get(streamName);
            if(tps == null){
                tps = new HashSet<>();
                streamTopicsMap.put(streamName, tps);
            }
            tps.add(topicName);
        }
        Set<String> res = new HashSet<>();
        for(Map.Entry<String, Set<String>> entry : streamTopicsMap.entrySet()){
            String streamName = entry.getKey();
            Collection<String> allTopics = adminClient.listTopics(streamName).names().get();
            for(String topic : entry.getValue()){
                if(allTopics.contains(topic)){
                    res.add(streamName + ":" + topic);
                }
            }
        }
        return res;
    }

    public Map<String, Integer> getNumPartitions(final Set<String> topics) {

      Map<String, Integer> result = new HashMap<>();

      try {
          KafkaFuture<Map<String, TopicDescription>> future = adminClient.describeTopics(filterUnexistingTopics(topics)).all();
        Map<String, TopicDescription> topicDescMap = future.get();
        for (String topicName : topicDescMap.keySet()) {
          result.put(topicName, topicDescMap.get(topicName).partitions().size());
        }
      } catch (Exception e) {
        throw new StreamsException(e);
      }
      return result;
    }

    private List<PartitionMetadata> topicPartitionListToMeta(List<TopicPartitionInfo> tpInfoList) {
      List<PartitionMetadata> pMetaList = new ArrayList<PartitionMetadata>();
      for (TopicPartitionInfo tpInfo : tpInfoList) {
        PartitionMetadata pMeta = new PartitionMetadata(null /*error*/, tpInfo.partition(), tpInfo.leader(),
                                                        tpInfo.replicas(), tpInfo.isr(), new ArrayList<Node>());
        pMetaList.add(pMeta);
      }
      return pMetaList;
    }

    private TopicMetadata topicDescToMeta(TopicDescription tDesc) {
      TopicMetadata tMeta = new TopicMetadata(null /*error*/, tDesc.name(), tDesc.isInternal(),
                                              topicPartitionListToMeta(tDesc.partitions()));
      return tMeta;
    }

    /**
     * Fetch the metadata for all topics in the default stream
     */
    public MetadataResponse fetchMetadata() {
        try {
          DescribeClusterResult result = adminClient.describeCluster(null);
          String clusterId = result.clusterId().get();
          List<Node> brokers = new ArrayList<Node>();
          brokers.addAll(result.nodes().get());

          List<TopicMetadata> topicMetaList = new ArrayList<TopicMetadata>();
          Map<String, TopicListing> topicsList = adminClient.listTopics((ListTopicsOptions) null).namesToListings().get();
          Map<String, TopicDescription> topicsDescMap = adminClient.describeTopics(topicsList.keySet(), null).all().get();

          for (String topicName : topicsDescMap.keySet()) {
            topicMetaList.add(topicDescToMeta(topicsDescMap.get(topicName)));
          }

          return new MetadataResponse(brokers, clusterId, result.controller().get().id(), topicMetaList);
        } catch (Exception e) {
          return null;
        }
    }

    /**
     * Check if the used brokers have version 0.10.1.x or higher.
     * <p>
     * Note, for <em>pre</em> 0.10.x brokers the broker version cannot be checked and the client will hang and retry
     * until it {@link StreamsConfig#REQUEST_TIMEOUT_MS_CONFIG times out}.
     *
     * @throws StreamsException if brokers have version 0.10.0.x
     */
    public void checkBrokerCompatibility(final boolean eosEnabled) throws StreamsException {
    }
}
