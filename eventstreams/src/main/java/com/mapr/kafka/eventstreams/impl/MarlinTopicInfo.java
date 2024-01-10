package com.mapr.kafka.eventstreams.impl;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;


/**
 * A topic name and feed number
 */
public final class MarlinTopicInfo {

    int nfeeds;
    private final String topic;

    public MarlinTopicInfo(String topic, int nfeeds) {
      this.topic = topic;
      this.nfeeds = nfeeds;
    }

    public List<PartitionInfo> getKafkaPartitionInfo() { 
      List<PartitionInfo> partitionInfo = new ArrayList();
      Node leader = new Node(0, "127.0.0.1", 7200);
      Node[] replicas = {leader};

      for (int feed = 0; feed < nfeeds; feed++) {
        partitionInfo.add(
            new PartitionInfo(topic, feed, leader, replicas, replicas));
      }
      
      return partitionInfo;
    }

}
