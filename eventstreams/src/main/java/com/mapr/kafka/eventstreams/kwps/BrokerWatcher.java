/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.mapr.fs.ServiceWatcher;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("unchecked")
public class BrokerWatcher extends ServiceWatcher {
  private static final ObjectMapper JSON_SERDE = new ObjectMapper();

  private static final String DEFAULT_CLUSTER = "default-cluster";

  private static final String BROKERS_PATH_FORMAT = "/kafka-wire-protocol/%s/brokers";

  private final String brokersPath;
  private final Map<String, String> connectionProperties = new HashMap<>();

  public BrokerWatcher(final String zkConnectString, final String zkClusterName) {
    this(zkConnectString, zkClusterName, Optional.empty());
  }

  public BrokerWatcher(final String zkConnectString, final String zkClusterName, final Optional<String> kafkaCluster) {
    super(zkConnectString, zkClusterName);

    final String kafkaClusterName = kafkaCluster.orElse(DEFAULT_CLUSTER);
    this.brokersPath = String.format(BROKERS_PATH_FORMAT, kafkaClusterName);
  }

  public Iterable<KBrokerDescriptor> getBrokers() throws Exception {
    ImmutableList.Builder<KBrokerDescriptor> builder = ImmutableList.builder();
    log.info("Brokers path: {}.", brokersPath);
    final ListIterator<String> brokersItr = getChildren(brokersPath).listIterator();
    while (brokersItr.hasNext()) {
      final KBrokerDescriptor broker = getBroker(brokersItr.next());
      if (broker != null) {
        builder.add(broker);
      }
    }
    return builder.build();
  }

  public Map<String, String> getConnectionProperties() throws Exception {
    StringBuilder bootstrapServers = new StringBuilder();
    log.info("Brokers path: {}.", brokersPath);
    final ListIterator<String> brokersItr = getChildren(brokersPath).listIterator();
    while (brokersItr.hasNext()) {
      readBrokerProps(brokersItr.next(), bootstrapServers);
    }
    if (bootstrapServers.length() > 0) {
      connectionProperties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.substring(1));
    }
    return connectionProperties;
  }
  
  private KBrokerDescriptor getBroker(String broker) {
    try {
      final byte[] data = getData(String.format("%s/%s", brokersPath, broker));
      final String jsonStr = Bytes.toString(data);
      final Map<String, String> brokerProps = JSON_SERDE.readValue(jsonStr, HashMap.class);
      final int brokerId = Integer.valueOf(broker.substring(broker.lastIndexOf('-')));    
      return new KBrokerDescriptor(brokerId, brokerProps);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  private void readBrokerProps(final String broker, final StringBuilder bootstrapServers) throws Exception {
    final byte[] data = getData(String.format("%s/%s", brokersPath, broker));
    final String jsonStr = Bytes.toString(data);
    final Map<String, String> brokerProps = JSON_SERDE.readValue(jsonStr, HashMap.class);

    final String serverPort = brokerProps.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (serverPort != null) {
      bootstrapServers.append(',').append(serverPort);
    }
    final String securityProtocol = brokerProps.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (securityProtocol != null) {
      connectionProperties.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    }
    final String saslMechanism = brokerProps.get(SaslConfigs.SASL_MECHANISM);
    if (saslMechanism != null) {
      connectionProperties.putIfAbsent(SaslConfigs.SASL_MECHANISM, saslMechanism);
    }
  }

}
