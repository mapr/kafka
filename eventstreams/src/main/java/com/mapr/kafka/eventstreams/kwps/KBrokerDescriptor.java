/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps;

import java.util.Map;

import lombok.Getter;
import lombok.experimental.Accessors;

@Accessors(chain = true, fluent = false)
public class KBrokerDescriptor {
  @Getter private final int id;
  @Getter private final Map<String, String> props;

  public KBrokerDescriptor(final int id, final Map<String, String> props) {
    this.id = id;
    this.props = props;
  }

}
