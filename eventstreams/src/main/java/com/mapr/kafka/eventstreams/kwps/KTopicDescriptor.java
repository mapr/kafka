/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true, fluent = false)
public class KTopicDescriptor {

  public static enum CompressionType {
    off, lzf, lz4, zlib
  }

  @Getter @NonNull private final String topic;
  @Getter @Setter private int partitions = -1;
  @Getter @Setter private long ttl = -1;
  @Getter @Setter private long size = -1;
  @Getter @Setter private String owner = null;
  @Getter @Setter private boolean ownVolume = false;
  @Getter private CompressionType compression = null;
  @Getter @Setter private String adminPerms = null;
  @Getter @Setter private String producePerms = null;
  @Getter @Setter private String consumePerms = null;

  public KTopicDescriptor(final String topicName) {
    this.topic = KafkaTopic.validate(topicName);
  }

  public String getCompressionString() {
    return String.valueOf(compression);
  }

  public KTopicDescriptor setCompression(@NonNull CompressionType compression) {
    this.compression = compression;
    return this;
  }

  public KTopicDescriptor setCompression(@NonNull String compression) {
    try {
      this.compression = CompressionType.valueOf(compression.toLowerCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("'" + compression + "' is not a supported compression scheme for topics.", e);
    }
    return this;
  }

  public boolean hasPartitions() {
    return partitions != -1;
  }

  public boolean hasCompression() {
    return compression != null;
  }

  public boolean hasAdminPerms() {
    return adminPerms != null;
  }

  public boolean hasProducePerms() {
    return producePerms != null;
  }

  public boolean hasConsumePerms() {
    return consumePerms != null;
  }

  public boolean hasOwner() {
    return owner != null;
  }

  public boolean hasTtl() {
    return ttl != -1;
  }

  public boolean hasSize() {
    return size != -1;
  }

}
