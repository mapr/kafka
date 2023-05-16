package com.mapr.kafka.eventstreams.impl;

import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.ConnectProtocol;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.WorkerRebalanceListener;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MarlinWorkerCoordinatorV10 extends MarlinWorkerCoordinator {
  private static final Logger log = LoggerFactory.getLogger(MarlinWorkerCoordinatorV10.class);
  private final ConfigBackingStore configBackingStore;

  public MarlinWorkerCoordinatorV10(DistributedConfig config, String groupId,
                                 String restUrl, ConfigBackingStore configBackingStore,
                                 WorkerRebalanceListener rebalanceCb) {
    super(config, groupId, restUrl, null /*KafkaConfigStorage*/, rebalanceCb);
    this.configBackingStore = configBackingStore;
    log.debug("MarlinWorkerCoordinatorV10 constructor");
  }

  @Override
  protected ClusterConfigState getConfigSnapshot() {
    return configBackingStore.snapshot();
  }

  @Override
  protected String getConfigTopic(Map<String, ?> configs) {
    return (String) configs.get(DistributedConfig.CONFIG_TOPIC_CONFIG);
  }
}
