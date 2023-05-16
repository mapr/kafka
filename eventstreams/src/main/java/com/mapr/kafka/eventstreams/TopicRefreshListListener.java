/* Copyright (c) 2018 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public interface TopicRefreshListListener {

  void updatedTopics(Set<TopicPartition> topicFeeds);

}
