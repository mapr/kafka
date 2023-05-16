/* Copyright (c) 2018 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl;

import com.mapr.kafka.eventstreams.TopicRefreshListListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public class TopicRefreshListListenerImpl implements TopicRefreshListListener {

  @Override
  public void updatedTopics(Set<TopicPartition> topicFeeds) {
  }

}
