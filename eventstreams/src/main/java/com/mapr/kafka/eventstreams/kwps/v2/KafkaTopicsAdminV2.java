/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps.v2;


import static com.mapr.kwps.KwpsCommon.KWPS_TOPICS_FOLDER;
import static com.mapr.kwps.KwpsCommon.KWPS_TOPICS_FOLDER_PATH;
import static com.mapr.kwps.KwpsCommon.KWPS_USER_MAPR;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import com.google.common.collect.ImmutableSet;
import com.mapr.db.exceptions.AccessDeniedException;
import com.mapr.fs.MapRFileStatus;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.jni.Errno;
import com.mapr.fs.proto.Security.CredentialsMsg;
import com.mapr.kafka.eventstreams.MarlinIOException;
import com.mapr.kafka.eventstreams.StreamDescriptor;
import com.mapr.kafka.eventstreams.Streams;
import com.mapr.kafka.eventstreams.TopicDescriptor;
import com.mapr.kafka.eventstreams.impl.admin.MStreamDescriptor;
import com.mapr.kafka.eventstreams.impl.admin.MarlinAdminClientImpl;
import com.mapr.kafka.eventstreams.impl.admin.MarlinAdminImpl;
import com.mapr.kafka.eventstreams.impl.admin.TopicFeedInfo;
import com.mapr.kwps.BrokerWatcher;
import com.mapr.kwps.BrokerDescriptor;
import com.mapr.kwps.KTopicDescriptor;
import com.mapr.kwps.KTopicDescriptor.CompressionType;
import com.mapr.kwps.KTopicsAdmin;
import com.mapr.kwps.KwpsCommon;
import com.mapr.kwps.VolumeManager;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaTopicsAdminV2 implements KTopicsAdmin, AutoCloseable {
  static private final Properties ADMIN_PROPS = new Properties();
  static {
    ADMIN_PROPS.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "fake");
  }

  protected MarlinAdminClientImpl adminClient;
  protected final MarlinAdminImpl maprAdmin;
  protected final MapRFileSystem mfs;
  protected final VolumeManager volMgr;

  protected final String maprUser;
  protected final String thisUser;

  public KafkaTopicsAdminV2() throws IOException {
    this(KWPS_USER_MAPR, null);
  }

  public KafkaTopicsAdminV2(@NonNull CredentialsMsg userCredentials) throws IOException {
    this(KWPS_USER_MAPR, userCredentials);
  }

  public KafkaTopicsAdminV2(@NonNull final String maprUser) throws IOException {
    this(maprUser, null);
  }

  public KafkaTopicsAdminV2(@NonNull final String maprUser, final CredentialsMsg userCredentials) throws IOException {
    this.maprUser = maprUser;
    this.adminClient = (MarlinAdminClientImpl) AdminClient.create(ADMIN_PROPS);
    this.maprAdmin = (MarlinAdminImpl) adminClient.getMapRAdmin();
    this.mfs = maprAdmin.getMaprfs();
    this.volMgr = new VolumeManager(mfs, userCredentials);
    this.thisUser = mfs.getUserInfo().getUserName();
    log.debug("KafkaTopicsAdmin user: {}, MapR user: {}", thisUser, maprUser);
  }

  public void close() throws IOException {
    adminClient.close();
  }

  public void createTopic(KTopicDescriptor ktopicDesc) throws IOException {
    final String topicName = ktopicDesc.getTopic();
    final String streamPath = KwpsCommon.getStreamPath(topicName);
    if (mfs.exists(new Path(streamPath))) {
      throw new TopicExistsException("The topic '" + topicName + "'" + "already exists.");
    }

    // Create volume if required
    final String volumePath = KwpsCommon.getStreamParentPath(topicName);
    final String volumeName = KwpsCommon.getVolumeName(topicName);
    if (ktopicDesc.isOwnVolume()) {
      volMgr.createVolume(volumeName, volumePath);
    } else {
      mfs.mkdirs(new Path(volumePath));
    }

    // Create Stream
    try {
      final StreamDescriptor streamDesc = Streams.newStreamDescriptor();
      if (ktopicDesc.hasTtl()) {
        streamDesc.setTimeToLiveSec(ktopicDesc.getTtl());
      }
      if (ktopicDesc.hasCompression()) {
        streamDesc.setCompressionAlgo(ktopicDesc.getCompression().name());
      } else {
        streamDesc.setCompressionAlgo(CompressionType.off.name());
      }

      ((MStreamDescriptor)streamDesc).setKafkaTopic(true); // mark the stream as KWPS
      streamDesc.setTopicPerms("u:" + thisUser + " | u:" + maprUser);
      if (ktopicDesc.hasAdminPerms()) {
        streamDesc.setAdminPerms(ktopicDesc.getAdminPerms());
      }
      if (ktopicDesc.hasConsumePerms()) {
        streamDesc.setConsumePerms(ktopicDesc.getConsumePerms());
      }
      if (ktopicDesc.hasProducePerms()) {
        streamDesc.setProducePerms(ktopicDesc.getProducePerms());
      }

      maprAdmin.createStream(streamPath, streamDesc);
    } catch (Exception e) {
      log.error("Failed to create stream: " + streamPath, e);
      cleanupCreate(ktopicDesc, volumePath);
      throw e;
    }

    // Create Topic
    try {
      final TopicDescriptor topicDesc = Streams.newTopicDescriptor();
      if (ktopicDesc.hasPartitions()) {
        topicDesc.setPartitions(ktopicDesc.getPartitions());
      }
      maprAdmin.createTopic(streamPath, topicName, topicDesc);
    } catch (Exception e) {
      log.error("Failed to create topic: " + topicName, e);
      cleanupCreate(ktopicDesc, volumePath);
      throw e;
    }
  }

  public void editTopic(KTopicDescriptor ktopicDesc) throws IOException {
    final String topicName = ktopicDesc.getTopic();
    final String streamPath = KwpsCommon.getStreamPath(topicName);
    if (!mfs.exists(new Path(streamPath))) {
      throw new UnknownTopicOrPartitionException("The topic '" + topicName + "' does not exist.");
    }

    // check if we need to update container Stream's properties
    try {
      if (ktopicDesc.hasTtl() || ktopicDesc.hasCompression()) {
        boolean updateStream = false;
        final StreamDescriptor streamDesc = Streams.newStreamDescriptor();
        if (ktopicDesc.hasTtl()) {
          updateStream = true;
          streamDesc.setTimeToLiveSec(ktopicDesc.getTtl());
        }
        if (ktopicDesc.hasCompression()) {
          streamDesc.setCompressionAlgo(ktopicDesc.getCompression().name());
        } else if (updateStream) {
          final StreamDescriptor oldSDesc = maprAdmin.getStreamDescriptor(streamPath);
          streamDesc.setCompressionAlgo(oldSDesc.getCompressionAlgo());
        }

        maprAdmin.editStream(streamPath, streamDesc);
      }

      if (ktopicDesc.hasPartitions()) {
        maprAdmin.editTopic(streamPath, topicName, ktopicDesc.getPartitions());
      }
    } catch (AccessDeniedException e) {
      throw (TopicAuthorizationException) new TopicAuthorizationException(
          "The current user does not have permission to edit topic " + topicName,
          ImmutableSet.of(topicName)).initCause(e);
    }
  }

  public KTopicDescriptor getTopicDescriptor(String topicName) throws IOException {
    return getTopicDescriptorInternal(topicName);
  }

  private KTopicDescriptor getTopicDescriptorInternal(String topicName) throws IOException {
    final KTopicDescriptor ktopicDesc = new KTopicDescriptor(topicName);
    final String streamPath = KwpsCommon.getStreamPath(topicName);
    try {
      final MapRFileStatus fStatus = mfs.getMapRFileStatus(new Path(streamPath));
      ktopicDesc.setOwner(fStatus.getOwner());
    } catch (FileNotFoundException | IllegalArgumentException e) {
      throw new UnknownTopicOrPartitionException("The topic '" + topicName + "' does not exist.", e);
    }

    // properties from Stream
    final StreamDescriptor streamDesc = maprAdmin.getStreamDescriptor(streamPath);
    ktopicDesc.setTtl(streamDesc.getTimeToLiveSec());
    ktopicDesc.setCompression(streamDesc.getCompressionAlgo());
    ktopicDesc.setAdminPerms(streamDesc.getAdminPerms());
    ktopicDesc.setConsumePerms(streamDesc.getConsumePerms());
    ktopicDesc.setAdminPerms(streamDesc.getProducePerms());

    // properties from Topic
    try {
      final TopicDescriptor topicDesc = maprAdmin.getTopicDescriptor(streamPath, topicName);
      ktopicDesc.setPartitions(topicDesc.getPartitions());

      // properties from Partitions
      final String topicFullName = streamPath + ":" + topicName;
      final List<TopicFeedInfo> tlist = maprAdmin.infoTopic(topicFullName);
      long topicSize = 0;
      for (TopicFeedInfo topicFeedInfo : tlist) {
        topicSize += topicFeedInfo.stat().getPhysicalSize();
      }
      ktopicDesc.setSize(topicSize);
    } catch (MarlinIOException e) {
       if (e.getErrorCode() == Errno.EACCES) {
        log.debug("Current user doesn't have topic perm!", e);
        // leave the default values for partition and size
        // {falls through}
       } else {
         throw e;
       }
    }

    // properties from volume
    final boolean isVol = volMgr.isVolume(topicName);
    ktopicDesc.setOwnVolume(isVol);

    return ktopicDesc;
  }

  public boolean deleteTopic(String topicName) throws IOException {
    final boolean isVol = volMgr.isVolume(topicName);
    if(isVol) {
      return volMgr.deleteVolume(topicName);
    } else {
      final Path volumePath = new Path(KwpsCommon.getStreamParentPath(topicName));
      if (!mfs.exists(volumePath)) {
        throw new UnknownTopicOrPartitionException("The topic '" + topicName + "' does not exist.");
      }
      return mfs.delete(volumePath, true);
    }
  }

  public Iterable<KTopicDescriptor> listTopics() throws IOException {
    return listTopics(null);
  }

  public Iterable<KTopicDescriptor> listTopics(String topicNameRegex) throws IOException {
    final boolean allTopics = topicNameRegex == null;
    final List<KTopicDescriptor> topicList = new LinkedList<>();
    MapRFileStatus[] all = null;
    try {
      all = mfs.listStatus(KWPS_TOPICS_FOLDER_PATH);
    } catch (FileNotFoundException e) {
      new IOException("Kafka topics folder '" + KWPS_TOPICS_FOLDER + "' doesn't exist on the cluster.", e);
    }

    final Pattern topicRegex = allTopics ? null : Pattern.compile(topicNameRegex);
    for (MapRFileStatus fStatus : all) {
      final Path fPath = fStatus.getPath();
      if (fStatus.isDirectory()) {
        final String topicName = fPath.getName();
        if (allTopics || topicRegex.matcher(topicName).matches()) {
          final KTopicDescriptor topicDesc = getTopicDescriptorInternal(topicName);
          topicList.add(topicDesc);
        }
      } else {
        log.warn("Unknown file '{}' in Kafka topics folder!", fPath);
      }
    }
    return topicList;
  }

  public Map<String, String> getConnectionProperties() throws IOException {
    return getConnectionProperties(Optional.empty());
  }

  public Map<String, String> getConnectionProperties(Optional<String> kafkaCluster) throws IOException {
    try (final BrokerWatcher watcher = newBrokerWatcher(kafkaCluster)) {
      watcher.connect();
      return watcher.getConnectionProperties();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  private void cleanupCreate(KTopicDescriptor ktopicDesc, String volumePath) throws IOException {
    try {
      if (ktopicDesc.isOwnVolume()) {
        volMgr.deleteVolume(ktopicDesc.getTopic());
      } else {
        mfs.delete(new Path(volumePath), true);
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to delete partial topic metadata.", e);
    }
  }

  public void linkTopic(final String topicName, final String topicTarget) throws IOException {
    throw new UnsupportedOperationException("linkTopic() isn't implemented yet.");
  }

  public BrokerDescriptor getController(Optional<String> kafkaCluster) throws IOException {
    try (final BrokerWatcher watcher = newBrokerWatcher(kafkaCluster)) {
      watcher.connect();
      return watcher.getController();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  public Iterable<BrokerDescriptor> listBrokers(Optional<String> kafkaCluster) throws IOException {
    try (final BrokerWatcher watcher = newBrokerWatcher(kafkaCluster)) {
      watcher.connect();
      return watcher.getBrokers();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  @Override
  public void notifyMetadataChange(Optional<String> kafkaCluster) throws IOException {
    try (final BrokerWatcher watcher = newBrokerWatcher(kafkaCluster)) {
      watcher.connect();
      watcher.notifyMetadataChange();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new IOException(e);
    }
  }

  private BrokerWatcher newBrokerWatcher(final Optional<String> kafkaCluster) throws IOException {
    final String zkClusterName = mfs.getDefaultClusterName();
    final String zkConnectString = mfs.getZkConnectString();
    return new BrokerWatcher(zkConnectString, zkClusterName, kafkaCluster);
  }

}
