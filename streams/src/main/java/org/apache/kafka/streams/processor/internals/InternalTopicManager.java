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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class InternalTopicManager {
    private final static String INTERRUPTED_ERROR_MESSAGE = "Thread got interrupted. This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA or dev-mailing list (https://kafka.apache.org/contact).";

    private final Logger log;
    private final long windowChangeLogAdditionalRetention;
    private final Map<String, String> defaultTopicConfigs = new HashMap<>();

    private final short replicationFactor;
    private final AdminClient adminClient;

    private final int retries;

    public InternalTopicManager(final AdminClient adminClient,
                                final StreamsConfig streamsConfig) {
        this.adminClient = adminClient;

        LogContext logContext = new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName()));
        log = logContext.logger(getClass());

        replicationFactor = streamsConfig.getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue();
        windowChangeLogAdditionalRetention = streamsConfig.getLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG);
        retries = new AdminClientConfig(streamsConfig.getAdminConfigs("dummy")).getInt(AdminClientConfig.RETRIES_CONFIG);

        log.debug("Configs:" + Utils.NL,
            "\t{} = {}" + Utils.NL,
            "\t{} = {}" + Utils.NL,
            "\t{} = {}",
            AdminClientConfig.RETRIES_CONFIG, retries,
            StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor,
            StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, windowChangeLogAdditionalRetention);

        for (final Map.Entry<String, Object> entry : streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX).entrySet()) {
            if (entry.getValue() != null) {
                defaultTopicConfigs.put(entry.getKey(), entry.getValue().toString());
            }
        }
    }

    /**
     * Prepares a set of given internal topics.
     *
     * If a topic does not exist creates a new topic.
     * If a topic with the correct number of partitions exists ignores it.
     * If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
     */
    public void makeReady(final Map<InternalTopicConfig, Integer> topics) {
        for (int i = 0; i < MAX_TOPIC_READY_TRY; i++) {
            try {
                final Set<String> topicsSet = new HashSet<String>();
                for (InternalTopicConfig topicConf : topics.keySet()) {
                  topicsSet.add(topicConf.name());
                }
                final Map<String, Integer> existingTopicPartitions = streamsKafkaClient.getNumPartitions(topicsSet);
                final Map<InternalTopicConfig, Integer> topicsToBeCreated = validateTopicPartitions(topics, existingTopicPartitions);
                if (topicsToBeCreated.size() > 0) {
                    streamsKafkaClient.createTopics(topicsToBeCreated, replicationFactor, windowChangeLogAdditionalRetention, null);
                }

                return;
            } while (remainingRetries-- > 0);

            final String timeoutAndRetryError = "Could not create topics. " +
                "This can happen if the Kafka cluster is temporary not available. " +
                "You can increase admin client config `retries` to be resilient against this error.";
            log.error(timeoutAndRetryError);
            throw new StreamsException(timeoutAndRetryError);
        }
    }

    /**
     * Get the number of partitions for the given topics
     */
    public Map<String, Integer> getNumPartitions(final Set<String> topics) {
      return streamsKafkaClient.getNumPartitions(topics);
    }

            if (retry) {
                topics.removeAll(existingNumberOfPartitionsPerTopic.keySet());
                continue;
            }

            return existingNumberOfPartitionsPerTopic;
        } while (remainingRetries-- > 0);

        return Collections.emptyMap();
    }

    /**
     * Check the existing topics to have correct number of partitions; and return the non existing topics to be created
     */
    private Set<InternalTopicConfig> validateTopicPartitions(final Collection<InternalTopicConfig> topicsPartitionsMap,
                                                             final Map<String, Integer> existingTopicNamesPartitions) {
        final Set<InternalTopicConfig> topicsToBeCreated = new HashSet<>();
        for (final InternalTopicConfig topic : topicsPartitionsMap) {
            final Integer numberOfPartitions = topic.numberOfPartitions();
            if (existingTopicNamesPartitions.containsKey(topic.name())) {
                if (!existingTopicNamesPartitions.get(topic.name()).equals(numberOfPartitions)) {
                    final String errorMsg = String.format("Existing internal topic %s has invalid partitions: " +
                            "expected: %d; actual: %d. " +
                            "Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.",
                        topic.name(), numberOfPartitions, existingTopicNamesPartitions.get(topic.name()));
                    log.error(errorMsg);
                    throw new StreamsException(errorMsg);
                }
            } else {
                topicsToBeCreated.add(topic);
            }
        }

        return topicsToBeCreated;
    }

}
