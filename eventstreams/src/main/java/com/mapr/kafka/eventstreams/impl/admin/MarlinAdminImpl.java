/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.admin;

import static org.ojai.DocumentConstants.ID_FIELD;
import static org.ojai.DocumentConstants.ID_KEY;
import static org.ojai.store.QueryCondition.Op.GREATER;
import static org.ojai.store.QueryCondition.Op.LESS;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.AccessControlException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.Errors;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentMutation;
import org.ojai.store.QueryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.db.FamilyDescriptor;
import com.mapr.db.FamilyDescriptor.Compression;
import com.mapr.db.Table;
import com.mapr.db.exceptions.TableNotFoundException;
import com.mapr.db.impl.FamilyDescriptorImpl;
import com.mapr.db.impl.MapRDBImpl;
import com.mapr.db.impl.TableDescriptorImpl;
import com.mapr.baseutils.utils.AceHelper;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.MapRTabletScanner;
import com.mapr.fs.jni.Errno;
import com.mapr.fs.jni.MapRConstants.ErrorValue;
import com.mapr.fs.jni.MapRUserInfo;
import com.mapr.fs.jni.MarlinJniAdmin;
import com.mapr.fs.proto.Common.FidMsg;
import com.mapr.fs.proto.Dbserver.TabletDesc;
import com.mapr.fs.proto.Marlinserver.LcPartitionStatus;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;
import com.mapr.fs.proto.Marlinserver.MarlinTopicMetaEntry;
import com.mapr.fs.proto.Marlinserver.TopicFeedStatInfo;
import com.mapr.fs.proto.Marlinserver.TopicFeedStatResponse;
import com.mapr.fs.proto.Marlincommon.CompactionConfig;
import com.mapr.fs.proto.Marlincommon.MarlinStreamDescriptor;
import com.mapr.fs.proto.Marlincommon.MarlinTableAttr;
import com.mapr.fs.proto.Marlincommon.MarlinTimestampType;
import com.mapr.fs.tables.CFPermissions;
import com.mapr.fs.tables.MapRAdmin;
import com.mapr.kafka.eventstreams.MarlinIOException;
import com.mapr.kafka.eventstreams.StreamDescriptor;
import com.mapr.kafka.eventstreams.TimestampType;
import com.mapr.kafka.eventstreams.TopicDescriptor;
import com.mapr.kafka.eventstreams.impl.MarlinRowKeyDecoder;

import lombok.Getter;

import com.mapr.fs.ErrnoException;

public class MarlinAdminImpl extends MarlinJniAdmin implements com.mapr.kafka.eventstreams.Admin {
  private static final Logger LOG = LoggerFactory.getLogger(MarlinAdminImpl.class);

  // misc constants
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_TTL_SECS = 7 * 24 * 3600; // 7 days
  private static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0; // No lag
  private static final long DEFAULT_COMPACTION_THROTTLE_FACTOR = 0; // No throttle
  private static final long DEFAULT_DELETE_RETENTION_MS = 1 * 24 * 3600 * 1000; // 1 day
  private static final long DEFAULT_PRODUCER_ID_EXPIRATION_SECS = 60 * 60 * 24 * 7; // 7 days
  private static final String COMPRESSION_RAW = "compression_raw";

  MarlinInternalDefaults mdef;
  com.mapr.db.Admin dbAdmin;
  MapRAdmin maprAdmin;
  @Getter
  MapRFileSystem maprfs;
  private static ExecutorService topicFeedStatService = null;
  private static int numAdmins = 0;

  public MarlinAdminImpl(Configuration conf) throws IOException {
    this(conf, null);
  }

  public MarlinAdminImpl(Configuration conf, String clusterName) throws IOException {
    try {
      URI uri = null;

      if (clusterName == null) {
        uri = new URI("maprfs:///");
      } else {
        conf.set("fs.default.name", "maprfs:///mapr/" + clusterName);
        uri = new URI("maprfs:///mapr/" + clusterName);
      }

      maprfs = new MapRFileSystem();
      maprfs.initialize(uri, conf);
    } catch (IOException | URISyntaxException e) {
      throw e instanceof IOException ? (IOException)e : new IOException(e);
    }
    /*
     * userInfo passed to OpenAdmin call here does not have userId and groupId
     * populated. userInfo is completely populated later when path is present
     * and passed to populateAndGetUserInfo/populateUserInfo call.
     */
    _clntPtr = OpenAdmin(maprfs.getUserInfo());
    if (_clntPtr == 0) {
      /**
       * <MAPR_ERROR>
       *   Unable to create MapRClient object for the cluster
       *   specified in hostname:port
       * </MAPR_ERROR>
       */
      throw new IOException("Could not create MarlinAdminImpl");
    }

    mdef = MarlinInternalDefaults.getDefaultInstance();
    dbAdmin = MapRDBImpl.newAdmin();

    maprAdmin = new MapRAdmin(maprfs);

    synchronized(MarlinAdminImpl.class) {
      if (topicFeedStatService == null)
        topicFeedStatService = Executors.newFixedThreadPool(16);

      numAdmins++;
    }
  }

  /*
   * Create a marlin stream.
   */
  @Override
  public void createStream(String streamName, StreamDescriptor gdesc)
      throws IOException, IllegalArgumentException {
    int err = 0;
    MStreamDescriptor sdesc = (MStreamDescriptor)gdesc;
    MarlinStreamDescriptor.Builder mdesc = MarlinStreamDescriptor.newBuilder();
    MarlinTableAttr.Builder mtattr = MarlinTableAttr.newBuilder();

    if (sdesc == null) {
      throw new IllegalArgumentException("MStreamDescriptor cannot be null");
    }
    validateStreamDesc(sdesc);

    mtattr.setKafkaTopic(sdesc.isKafkaTopic());

    if (sdesc.hasDefaultPartitions()) {
      mtattr.setDefaultNumFeedsPerTopic(sdesc.getDefaultPartitions());
    } else {
      mtattr.setDefaultNumFeedsPerTopic(DEFAULT_PARTITIONS);
    }

    if (sdesc.hasTimeToLiveSec()) {
      mdesc.setTtlSecs(sdesc.getTimeToLiveSec());
    } else {
      mdesc.setTtlSecs(DEFAULT_TTL_SECS);
    }

    if (sdesc.hasCompressionAlgo()) {
      mdesc.setCompressionAlgo(FamilyDescriptorImpl.compression2Proto(compressionNameToType(sdesc.getCompressionAlgo())));
    }

    if (sdesc.hasClientCompression()) {
      mdesc.setClientCompression(sdesc.getClientCompression());
    } else {
      mdesc.setClientCompression(true);
    }

    if (sdesc.hasAutoCreateTopics()) {
      mtattr.setAutoCreateTopics(sdesc.getAutoCreateTopics());
    } else {
      mtattr.setAutoCreateTopics(true);
    }

    if (sdesc.isKafkaTopic()) {
      mtattr.setAutoCreateTopics(false);
    }

    if (sdesc.hasProducePerms()) {
      mdesc.setProducePerms(AceHelper.toPostfix(sdesc.getProducePerms()));
    }

    if (sdesc.hasConsumePerms()) {
      mdesc.setConsumePerms(AceHelper.toPostfix(sdesc.getConsumePerms()));
    }

    if (sdesc.hasTopicPerms()) {
      mdesc.setTopicPerms(AceHelper.toPostfix(sdesc.getTopicPerms()));
    }

    if (sdesc.hasCopyPerms()) {
      mdesc.setCopyPerms(AceHelper.toPostfix(sdesc.getCopyPerms()));
    }

    if (sdesc.hasAdminPerms()) {
      mdesc.setAdminPerms(AceHelper.toPostfix(sdesc.getAdminPerms()));
    }

    if (sdesc.hasIsChangelog()) {
      mtattr.setIsChangelog(sdesc.getIsChangelog());
    } else {
      mtattr.setIsChangelog(false);
    }

    if (sdesc.hasTimestampType()) {
      mtattr.setDefaultTimestampType(timestampTypeToMarlinTimestampType(sdesc.getDefaultTimestampType()));
    } else {
      mtattr.setDefaultTimestampType(mdef.getDefaultTimestampType());
    }

    if (sdesc.hasCompact()) {
      if (sdesc.getCompact()) {
        if (sdesc.getForce()) {
          mtattr.setCompactionConfig(CompactionConfig.CompactionForceEnable);
        } else {
          mtattr.setCompactionConfig(CompactionConfig.CompactionEnable);
        }
      } else {
        mtattr.setCompactionConfig(CompactionConfig.CompactionDisable);
      }
    } else {
      mtattr.setCompactionConfig(CompactionConfig.CompactionDisable);
    }

    if (sdesc.hasMinCompactionLagMS()) {
      mtattr.setMinCompactionLagMS(sdesc.getMinCompactionLagMS());
    } else {
      mtattr.setMinCompactionLagMS(DEFAULT_MIN_COMPACTION_LAG_MS);
    }

    if (sdesc.hasDeleteRetentionMS()) {
      mtattr.setDeleteRetentionMS(sdesc.getDeleteRetentionMS());
    } else {
      mtattr.setDeleteRetentionMS(DEFAULT_DELETE_RETENTION_MS);
    }

    if (sdesc.hasCompactionThrottleFactor()) {
      mtattr.setCompactionThrottleFactor(sdesc.getCompactionThrottleFactor());
    } else {
      mtattr.setCompactionThrottleFactor(DEFAULT_COMPACTION_THROTTLE_FACTOR);
    }

    if (sdesc.hasProducerIdExpirySecs()) {
      mtattr.setProducerIdExpirySecs(sdesc.getProducerIdExpirySecs());
    } else {
      mtattr.setProducerIdExpirySecs(DEFAULT_PRODUCER_ID_EXPIRATION_SECS);
    }

    mdesc.setMarlinAttr(mtattr.build());

    MapRUserInfo userInfo = MapRFileSystem.CurrentUserInfo();

    err = CreateStream(_clntPtr, streamName, mdesc.build().toByteArray(),
                       userInfo);

    if (err == Errno.EINVAL) {
      throw new IllegalArgumentException("Create stream failed with error : " +
                                         Errno.toString(err) + " (" + err + ")");
    } else if (err != 0) {
      throw new MarlinIOException("Create stream failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);

    }
  }

  /*
   * Modify the attributes of a marlin stream.
   */
  @Override
  public void editStream(String streamName, StreamDescriptor gdesc)
      throws IOException, IllegalArgumentException, AccessControlException {

    MStreamDescriptor sdesc = (MStreamDescriptor)gdesc;
    if (streamName == null)
      throw new IllegalArgumentException("streamName cannot be null");

    if (sdesc == null)
      throw new IllegalArgumentException("MStreamDescriptor cannot be null");

    validateStreamDesc(sdesc);
    TableDescriptorImpl desc = checkStreamAndGetTableDescriptor(streamName);
    TableDescriptorImpl modDesc = 
      (TableDescriptorImpl)MapRDBImpl.newTableDescriptor();
    boolean alterTable = false;

    boolean isChangelog = desc.isChangelog();

    FamilyDescriptor fMsgs = MapRDBImpl.newFamilyDescriptor();
    ((FamilyDescriptorImpl) fMsgs).clearInMemory();
    boolean alterFamily = false;

    if (sdesc.hasDefaultPartitions()) {
      if (isChangelog) {
        throw new IllegalArgumentException("Changelog stream partition number cannot be changed");
      }

      int numPartitionsPerTopic = sdesc.getDefaultPartitions();
      modDesc.setStreamDefaultPartitions(numPartitionsPerTopic);
      alterTable = true;
    }

    if (sdesc.hasTimeToLiveSec()) {
      long timeToLiveSecs = sdesc.getTimeToLiveSec();
      fMsgs.setTTL(timeToLiveSecs);
      alterFamily = true;
    }

    if (sdesc.hasAutoCreateTopics()) {
      if (isChangelog) {
        throw new IllegalArgumentException("Changelog stream autoCreateTopics cannot be changed");
      }

      boolean autoCreateTopics = sdesc.getAutoCreateTopics();
      modDesc.setStreamAutoCreate(autoCreateTopics);
      alterTable = true;
    }

    if (sdesc.hasCompressionAlgo()) {
      Compression ctype = compressionNameToType(sdesc.getCompressionAlgo());
      fMsgs.setCompression(ctype);
      alterFamily = true;
    }

    if (sdesc.hasClientCompression()) {
      boolean val = sdesc.getClientCompression();
      modDesc.setClientCompression(val);
      alterTable = true;
    }

    if (sdesc.hasTimestampType()) {
      MarlinTimestampType timestampType = timestampTypeToMarlinTimestampType(sdesc.getDefaultTimestampType());
      modDesc.setStreamTimestampType(timestampType);
      alterTable = true;
    }

    if (sdesc.hasCompact()) {
      boolean val = sdesc.getCompact();
      if (desc.getCompact() != val) {
        modDesc.setCompact(val, sdesc.getForce());
        alterTable = true;
      }
    }

    if (sdesc.hasMinCompactionLagMS()) {
      long minCompactionLagMS = sdesc.getMinCompactionLagMS();
      if (minCompactionLagMS < 0)
        throw new IllegalArgumentException("minlogcompactionlag must be between [0, " + Long.MAX_VALUE + "]");
      modDesc.setStreamMinCompactionLagMS(minCompactionLagMS);
      alterTable = true;
    }

    if (sdesc.hasCompactionThrottleFactor()) {
      long compactionThrottleFactor = sdesc.getCompactionThrottleFactor();
      if (compactionThrottleFactor < 0)
        throw new IllegalArgumentException("throttle factor must be between [0, " + Long.MAX_VALUE + "]");
      modDesc.setStreamCompactionThrottleFactor(compactionThrottleFactor);
      alterTable = true;
    }

    if (sdesc.hasDeleteRetentionMS()) {
      long deleteRetentionMS = sdesc.getDeleteRetentionMS();
      if (deleteRetentionMS < 0)
        throw new IllegalArgumentException("deleteRetention must be between [0, " + Long.MAX_VALUE + "]");
      modDesc.setStreamDeleteRetentionMS(deleteRetentionMS);
      alterTable = true;
    }

    if (sdesc.hasProducerIdExpirySecs()) {
      modDesc.setProducerIdExpirySecs(sdesc.getProducerIdExpirySecs());
      alterTable = true;
    }

    // alter the table, if reqd
    if (alterTable) {
      modDesc.setPath(streamName);
      // keep the same families (alterTable() does not allow modifying families)
      // and will fail here if we don't keep the same families.
      modDesc.setFamilies(desc.getFamilies());
      try {
        dbAdmin.alterTable(modDesc);
      } catch (Exception e) {
        int errNo = -1;
        if ((e.getCause()!= null) && (e.getCause() instanceof ErrnoException)) {
          errNo = ((ErrnoException)e.getCause()).getErrno();
          switch (errNo) {
            case Errno.EPROTONOSUPPORT:
              throw new MarlinIOException("Log compaction can be enabled only on "
                                       + "streams created with MapR 6.1 and greater. "
                                       + "Please use the force option to override this "
                                       + "behavior")
              .setErrorCode(errNo);
            case Errno.ENOTCONN:
              throw new MarlinIOException("Log compaction can not be enabled as "
                                       + "gateway service is down or not responding, "
                                       + "please ensure that the gateway service is available and retry.")
              .setErrorCode(errNo);
            case Errno.EACCES:
              throw new AccessControlException("Permission denied for edit on stream "
                                               + streamName);
            default:
          }
        }
        throw new MarlinIOException(e.getMessage()).setErrorCode(errNo);
      } // end catch
    } // end if

    // alter the family, if reqd
    if (alterFamily)
      dbAdmin.alterFamily(new Path(streamName), mdef.getCfMessages(), fMsgs);

    // handle perm changes
    if (sdesc.hasAdminPerms() ||
        sdesc.hasProducePerms() ||
        sdesc.hasConsumePerms() ||
        sdesc.hasTopicPerms() ||
        sdesc.hasCopyPerms() ||
        sdesc.hasAdminPerms()) {

      MStreamDescriptor oldDesc = new MStreamDescriptor();
      populatePermissionsFromTable(streamName, oldDesc);
      if (!oldDesc.hasAdminPerms())
        throw new IOException("Access denied for table permissions");

      MapRUserInfo userInfo = maprfs.populateAndGetUserInfo(new Path(streamName));
      Map<String, String> tablePerms = buildTablePermissions(sdesc);
      List<CFPermissions> cfPerms = buildFamilyPermissions(sdesc, oldDesc,
                                                           userInfo.GetUserID());

      setPermissions(streamName, tablePerms, cfPerms);
    }
  }

  /*
   * Get the StreamDescriptor for a marlin stream.
   */
  @Override
  public StreamDescriptor getStreamDescriptor(String streamName)
      throws IOException, IllegalArgumentException {

    TableDescriptorImpl desc = checkStreamAndGetTableDescriptor(streamName);

    // Fill up MStreamDescriptor
    MStreamDescriptor sdesc = new MStreamDescriptor();
    sdesc.setAutoCreateTopics(desc.isStreamAutoCreate());
    sdesc.setDefaultPartitions(desc.getStreamDefaultPartitions());
    sdesc.setIsChangelog(desc.isChangelog());
    sdesc.setClientCompression(desc.getClientCompression());

    FamilyDescriptor fMsgs = desc.getFamily(mdef.getCfMessages());
    sdesc.setTimeToLiveSec(fMsgs.getTTL());
    sdesc.setCompressionAlgo(compressionTypeToName(fMsgs.getCompression()));
    if (desc.getStreamTimestampType() != MarlinTimestampType.NoTimestampType) {
      sdesc.setDefaultTimestampType(marlinTimestampTypeToTimestampType(desc.getStreamTimestampType()));
    }
    sdesc.setCompact(desc.getCompact());
    sdesc.setMinCompactionLagMS(desc.getStreamMinCompactionLagMS());
    sdesc.setCompactionThrottleFactor(desc.getStreamCompactionThrottleFactor());
    sdesc.setDeleteRetentionMS(desc.getStreamDeleteRetentionMS());

    sdesc.setProducerIdExpirySecs(desc.getProducerIdExpirySecs());
    sdesc.setKafkaTopic(desc.isKafkaTopic());
    populatePermissionsFromTable(streamName, sdesc);
    return (StreamDescriptor)sdesc;
  }

  /*
   * TODO JJS This is a temporary function and should be removed once JSON DB
   * Admin Api is done (Bug 19295).
   * Create a marlin stream as a CopyTable destination for Replication
   */
  public static void createStreamForCopy(String replicaStreamName,
                                         String srcStreamName)
      throws IOException, IllegalArgumentException {
    MarlinAdminImpl madmin = new MarlinAdminImpl(new Configuration());
    madmin.createStream(replicaStreamName,
                        madmin.getStreamDescriptor(srcStreamName));
  }

  /*
   * Delete a marlin stream.
   */
  @Override
  public void deleteStream(String streamName)
      throws IOException, IllegalArgumentException {

    checkStreamAndGetTableDescriptor(streamName);
    dbAdmin.deleteTable(streamName);
  }

  /*
   * Count the number of topics in a stream.
   */
  @Override
  public int countTopics(String streamName)
      throws IOException, IllegalArgumentException {
    checkStreamAndGetTableDescriptor(streamName);

    int numTopics = 0;

    QueryCondition condition = MapRDBImpl.newCondition()
        .and()
          .is(ID_FIELD, GREATER, mdef.getKeyPrefixTopicMeta())
          .is(ID_FIELD, LESS, mdef.getKeyPrefixTopicMetaEnd())
        .close()
    .build();

    try (Table table = MapRDBImpl.getTable(new Path(streamName));
        DocumentStream recStream = table.find(condition, mdef.getCfTopicMeta())) {

      Iterator<Document> iter = recStream.iterator();
      while (iter.hasNext()) {
        Document rec = iter.next();

        MarlinTopicMetaEntry tmeta = jsonRecToTopicMeta(rec);
        if (tmeta.getIsDeleted())
          continue;

        ++numTopics;
      }
    }
    return numTopics;
  }

  private Compression compressionNameToType(String name) {
    if (name.equalsIgnoreCase("off"))
      return Compression.None;
    else if (name.equalsIgnoreCase("lz4"))
      return Compression.LZ4;
    else if (name.equalsIgnoreCase("lzf"))
      return Compression.LZF;
    else if (name.equalsIgnoreCase("zlib"))
      return Compression.ZLIB;
    else
      throw new IllegalArgumentException("Unknown compression type " + name);
  }

  private String compressionTypeToName(Compression ctype) {
    String name;
    switch (ctype) {
    case LZ4:
      name = "lz4";
      break;

    case LZF:
      name = "lzf";
      break;
      
    case ZLIB:
      name = "zlib";
      break;

    case None:
    default:
      name = "off";
      break;
    }
    return name;  
  }

  private MarlinTimestampType timestampTypeToMarlinTimestampType(TimestampType type) {
    switch (type) {
    case CREATE_TIME:
      return MarlinTimestampType.CreateTime;
    case LOG_APPEND_TIME:
      return MarlinTimestampType.LogAppendTime;
    default:
      throw new IllegalArgumentException("Unknown timestamptype " + type);
    }
  }

  private TimestampType marlinTimestampTypeToTimestampType(MarlinTimestampType marlinType) {
    TimestampType type;
    switch (marlinType) {
    case CreateTime:
      type = TimestampType.CREATE_TIME;
      break;

    case LogAppendTime:
      type = TimestampType.LOG_APPEND_TIME;
      break;

    case NoTimestampType:
    default:
      throw new IllegalArgumentException("Invalid marlintimestamptype " + marlinType);
    }
    return type;
  }

  /*
   * validate all the fields set in the StreamDescriptor
   */
  private void validateStreamDesc(MStreamDescriptor sdesc)
      throws IOException {

    if (sdesc.hasDefaultPartitions()) {
      int numPartitions = sdesc.getDefaultPartitions();
      if (numPartitions < 1 || numPartitions > mdef.getMaxFeedsPerTopic())
        throw new IllegalArgumentException("defaultPartitions has an " +
            "invalid value " + numPartitions + ", it must between 1 and " +
            mdef.getMaxFeedsPerTopic());
    }

    if (sdesc.hasTimeToLiveSec()) {
      long ttl = sdesc.getTimeToLiveSec();
      if (ttl < 0)
        throw new IllegalArgumentException("timeToLive has an " +
            "invalid value " + ttl + ", it must be >= 0");
    }

    if (sdesc.hasCompressionAlgo())
      compressionNameToType(sdesc.getCompressionAlgo());

    if (sdesc.hasProducePerms())
      validateAceExpression(sdesc.getProducePerms());

    if (sdesc.hasConsumePerms())
      validateAceExpression(sdesc.getConsumePerms());

    if (sdesc.hasTopicPerms())
      validateAceExpression(sdesc.getTopicPerms());

    if (sdesc.hasCopyPerms())
      validateAceExpression(sdesc.getCopyPerms());

    if (sdesc.hasAdminPerms())
      validateAceExpression(sdesc.getAdminPerms());
  }

  private void validateAceExpression(String aceExpression) throws IOException {
    // This does a validation of the expression too.
    AceHelper.toPostfix(aceExpression);
  }

  private String[] getSplits(int numSplits) {
    String[] splits = new String[numSplits];

    for (int i = 0; i < numSplits; ++i) {
      splits[i] = String.format(mdef.getKeyPrefixFeedId() +
                                mdef.getKeyFmtFeedId(),
                                i);
    }
    return splits;
  }

  /*
   * Verifies that the given path is a stream, and returns the corresponsing
   * table descriptor.
   */
  private TableDescriptorImpl checkStreamAndGetTableDescriptor(String streamName)
     throws IllegalArgumentException, IOException {

    TableDescriptorImpl desc;
    desc = (TableDescriptorImpl)dbAdmin.getTableDescriptor(streamName);
    if (!desc.isStream())
      throw new IllegalArgumentException(streamName + " is not a stream");

    return desc;
  }

  @Override
  public boolean streamExists(String streamPath)
      throws IOException {
    try {
      TableDescriptorImpl tdesc = checkStreamAndGetTableDescriptor(streamPath);
      return true;
    } catch (IllegalArgumentException | TableNotFoundException e) {
      return false;
    }
  }

  /*
   * Returns a map (aceName -> aceExpression) for tablePermissions that need 
   * to be modified based on the stream descriptor.
   */
  private Map<String, String>
  buildTablePermissions(MStreamDescriptor sdesc) {
    Map<String, String> tperms = new HashMap<String, String>();

    if (!sdesc.hasAdminPerms()) {
      // no changes reqd in table perms
      return tperms;
    }

    // set all table-level perms to be the same as the new 'admin' perms
    String adminPerms = sdesc.getAdminPerms();
    for (String aceName : AceHelper.tblPermissionMap.values())
      tperms.put(aceName, adminPerms);
    return tperms;
  }

  private String mergeAces(String ace1, String ace2) {
    String mergedAce;

    if (ace1.length() == 0)
      mergedAce = ace2;
    else if (ace2.length() == 0)
      mergedAce = ace1;
    else if (ace1.equalsIgnoreCase(ace2))
      mergedAce = ace1;
    else if (ace1.equals("p") || ace2.equals("p"))
      mergedAce = "p";
    else
      mergedAce = "(" + ace1 + ")|(" + ace2 + ")";

    LOG.debug("Merged " + ace1 + " + " + ace2 + " to -> " + mergedAce);
    return mergedAce;
  }

  /*
   * Returns a list for cf permissions that need 
   * to be modified based on the stream descriptor.
   */
  private List<CFPermissions>
  buildFamilyPermissions(MStreamDescriptor newDesc,
                         MStreamDescriptor oldDesc,
                         int userId) {
    String user = new String("u:" + userId);

    // get producers ace
    String producers;
    if (newDesc.hasProducePerms())
      producers = newDesc.getProducePerms();
    else if (oldDesc != null)
      producers = oldDesc.getProducePerms();
    else
      producers = user;

    // get listeners ace
    String listeners;
    if (newDesc.hasConsumePerms())
      listeners = newDesc.getConsumePerms();
    else if (oldDesc != null)
      listeners = oldDesc.getConsumePerms();
    else
      listeners = user;

    // get topicEditors ace
    String topicEditors;
    if (newDesc.hasTopicPerms())
      topicEditors = newDesc.getTopicPerms();
    else if (oldDesc != null)
      topicEditors = oldDesc.getTopicPerms();
    else
      topicEditors = user;

    // get copiers ace
    String copiers;
    if (newDesc.hasCopyPerms())
      copiers = newDesc.getCopyPerms();
    else if (oldDesc != null)
      copiers = oldDesc.getCopyPerms();
    else
      copiers = user;

    // get admins ace
    String admins;
    if (newDesc.hasAdminPerms())
      admins = newDesc.getAdminPerms();
    else if (oldDesc != null)
      admins = oldDesc.getAdminPerms();
    else
      admins = user;

    // union of producers+listeners+admins+topicEditors+copiers
    String allList = mergeAces(producers, listeners);
    if (!copiers.equals(producers) &&
        !copiers.equals(listeners))
      allList = mergeAces(allList, copiers);
    if (!topicEditors.equals(producers) &&
        !topicEditors.equals(listeners) &&
        !topicEditors.equals(copiers))
      allList = mergeAces(allList, topicEditors);
    if (!admins.equals(producers) &&
        !admins.equals(listeners) &&
        !admins.equals(copiers) &&
        !admins.equals(topicEditors))
      allList = mergeAces(allList, admins);

    /*
     * 1. "messages" family
     *     - readperm : listeners | copiers
     *     - writeperm : producers | copiers
     *     - compressionperm : admins
     */
    CFPermissions msgsFamAces = new CFPermissions(mdef.getCfMessages());
    msgsFamAces.addCFPermission("readperm", mergeAces(listeners, copiers));
    msgsFamAces.addCFPermission("writeperm", mergeAces(producers, copiers));
    msgsFamAces.addCFPermission("compressionperm", admins);

    /*
     * 2. "topicMeta" family
     *     - readperm : producers | listeners | topic-editors | copiers | admin
     *     - writeperm : topic-editors | copiers
     */
    CFPermissions metaFamAces = new CFPermissions(mdef.getCfTopicMeta());

    metaFamAces.addCFPermission("readperm", allList);
    metaFamAces.addCFPermission("writeperm", mergeAces(topicEditors, copiers));

    /*
     * 3. "cursors" family
     *     - readperm : producers | listeners | topic-editors | copiers | admin
     *     - writeperm : listeners | copiers
     */
    CFPermissions cursorsFamAces = new CFPermissions(mdef.getCfCursors());
    cursorsFamAces.addCFPermission("readperm", allList);
    cursorsFamAces.addCFPermission("writeperm", mergeAces(listeners, copiers));

    /*
     * 4. "feedAssigns" family
     *     - readperm : producers | listeners | topic-editors | copiers | admin
     *     - writeperm : isteners | copiers
     */
    CFPermissions assignFamAces = new CFPermissions(mdef.getCfFeedAssigns());
    assignFamAces.addCFPermission("readperm", allList);
    assignFamAces.addCFPermission("writeperm", mergeAces(listeners, copiers));

    /*
     * save the configured values in traverseperm
     *   producers -> messages family
     *   listeners -> cursors family
     *   topicEditors -> topicMeta family
     *   copiers -> assign family
     */
    msgsFamAces.addCFPermission("traverseperm", producers);
    cursorsFamAces.addCFPermission("traverseperm", listeners);
    metaFamAces.addCFPermission("traverseperm", topicEditors);
    assignFamAces.addCFPermission("traverseperm", copiers);

    List<CFPermissions> cfPermsList = new ArrayList<CFPermissions>(4);
    cfPermsList.add(msgsFamAces);
    cfPermsList.add(metaFamAces);
    cfPermsList.add(cursorsFamAces);
    cfPermsList.add(assignFamAces);
    return cfPermsList;
  }

  /*
   * Set the required table & cf permissions on the table.
   */
  private void setPermissions(String streamName,
                              Map<String, String> tablePermissions,
                              List<CFPermissions> cfPermissions) 
      throws IOException {

    Path streamPath = new Path(streamName);

    // Set family-level permissions
    for (CFPermissions cfPerm : cfPermissions)
      maprAdmin.setFamilyPermissions(streamPath, cfPerm.getFamily(), cfPerm);

    // Set table-level permissions
    if (!tablePermissions.isEmpty())
      maprAdmin.setTablePermissions(streamPath, tablePermissions);
  }

  /*
   * Populate the stream descriptor based on the ace values defined on the 
   * table and its cfs.
   */
  private void populatePermissionsFromTable(String streamName,
                                            MStreamDescriptor sdesc)
      throws IOException {

    Path streamPath = new Path(streamName);
    Map<String, String> tablePerms = maprAdmin.getTablePermissions(streamPath);
    if (tablePerms.get("adminaccessperm") == null) {
      // Access denied for table permissions
      return;
    }

    sdesc.setAdminPerms(tablePerms.get("adminaccessperm"));

    String traversePermsName = new String("traverseperm");
    List<CFPermissions> cfPerms = maprAdmin.getFamilyPermissions(streamPath);
    for (CFPermissions cfp : cfPerms) {
      String cfName = cfp.getFamily();
      Map<String, String> pmap = cfp.getCfPermissions();

      if (cfName.equals(mdef.getCfMessages()))
        sdesc.setProducePerms(pmap.get(traversePermsName));
      else if (cfName.equals(mdef.getCfCursors()))
        sdesc.setConsumePerms(pmap.get(traversePermsName));
      else if (cfName.equals(mdef.getCfTopicMeta()))
        sdesc.setTopicPerms(pmap.get(traversePermsName));
      else if (cfName.equals(mdef.getCfFeedAssigns()))
        sdesc.setCopyPerms(pmap.get(traversePermsName));
    }
  }

  @Override
  public void createTopic(String streamPath, String topicName)
      throws IOException {

    maprfs.populateUserInfo(new Path(streamPath));
    String topicFullName = streamPath + ':' + topicName;
    MarlinTimestampType type = MarlinTimestampType.NoTimestampType;
    int err = CreateTopicWithDefaultFeeds(_clntPtr, topicFullName,
                                          type.getNumber());
    if (err == Errno.EEXIST) {
      throw new FileAlreadyExistsException("Topic " + topicFullName + " already exists.");
    }

    if (err == Errno.EINVAL) {
      throw new InvalidTopicException("Topic name \"" + topicName + "\" is illegal");
    }

    if (err != 0)
      throw new MarlinIOException("Create topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
  }

  @Override
  public void createTopic(String streamPath, String topicName,
                          int nfeeds) throws IOException {

    maprfs.populateUserInfo(new Path(streamPath));
    String topicFullName = streamPath + ':' + topicName;
    MarlinTimestampType type = MarlinTimestampType.NoTimestampType;

    if (nfeeds > mdef.getMaxFeedsPerTopic()) {
      throw new IllegalArgumentException("Number of partitions cannot be greater than " + mdef.getMaxFeedsPerTopic());
    }

    final int err;
    if (nfeeds == -1) { // org.apache.kafka.clients.admin.NewTopic.NO_PARTITIONS
      err = CreateTopicWithDefaultFeeds(_clntPtr, topicFullName, type.getNumber());
    } else {
      err = CreateTopic(_clntPtr, topicFullName, nfeeds, type.getNumber());
    }

    if (err == Errno.EEXIST) {
      throw new FileAlreadyExistsException("Topic " + topicFullName + " already exists.");
    }

    if (err == Errno.EINVAL) {
      throw new InvalidTopicException("Topic name \"" + topicName + "\" is illegal");
    }

    if (err != 0)
      throw new MarlinIOException("Create topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
  }

  @Override
  public void createTopic(String streamPath, String topicName,
                          TopicDescriptor desc) throws IOException {
    maprfs.populateUserInfo(new Path(streamPath));
    String topicFullName = streamPath + ':' + topicName;
    MTopicDescriptor mdesc = (MTopicDescriptor)desc;
    MarlinTimestampType type = MarlinTimestampType.NoTimestampType;
    if (mdesc.hasTimestampType()) {
      type = timestampTypeToMarlinTimestampType(mdesc.getTimestampType());
    }
    int err = 0;
    if (mdesc.hasPartitions()) {
      int nfeeds = mdesc.getPartitions();
      if (nfeeds < 1) {
        throw new IllegalArgumentException("Default number of partitions must be greater than or equal to 1");
      }

      if (nfeeds > mdef.getMaxFeedsPerTopic()) {
        throw new IllegalArgumentException("Number of partitions cannot be greater than " + mdef.getMaxFeedsPerTopic());
      }

      err = CreateTopic(_clntPtr, topicFullName, nfeeds, type.getNumber());
    } else {
      err = CreateTopicWithDefaultFeeds(_clntPtr, topicFullName, type.getNumber());
    }

    if (err == Errno.EEXIST) {
      throw new FileAlreadyExistsException("Topic " + topicFullName + " already exists.");
    }

    if (err == Errno.EINVAL) {
      throw new InvalidTopicException("Topic name \"" + topicName + "\" is illegal");
    }

    if (err != 0)
      throw new MarlinIOException("Create topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
  }

  @Override
  public void compactTopicNow(String streamPath, String topicName) throws IOException {
    maprfs.populateUserInfo(new Path(streamPath));

    TableDescriptorImpl desc = checkStreamAndGetTableDescriptor(streamPath);
    if (desc.isChangelog()) {
      throw new IllegalArgumentException("Changelog topic cannot be force compacted.");
    }

    if (desc.getCompact() == false) {
      throw new IllegalArgumentException("Log compaction is not enabled on stream." + streamPath );
    }

    String topicFullName = streamPath + ':' + topicName;
    int err = CompactTopic(_clntPtr, topicFullName);

    if (err == Errno.ENOENT) {
      throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
    }

    if (err != 0) {
      throw new MarlinIOException("Compact topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
    }
  }

  @Override
  public void editTopic(String streamPath, String topicName,
                        int nfeeds) throws IOException {

    maprfs.populateUserInfo(new Path(streamPath));

    TableDescriptorImpl desc = checkStreamAndGetTableDescriptor(streamPath);
    boolean isChangelog = desc.isChangelog();
    if (isChangelog) {
      throw new IllegalArgumentException("Changelog topic partition number cannot be changed");
    }

    if (nfeeds < 1) {
      throw new IllegalArgumentException("Number of partitions in a topic must be greater than or equal to 1");
    }

    if (nfeeds > mdef.getMaxFeedsPerTopic()) {
      throw new IllegalArgumentException("Number of partitions cannot be greater than " + mdef.getMaxFeedsPerTopic());
    }

    String topicFullName = streamPath + ':' + topicName;
    MarlinTimestampType type = MarlinTimestampType.NoTimestampType;
    int err = EditTopic(_clntPtr, topicFullName, nfeeds, type.getNumber());

    if (err == Errno.ENOENT) {
      throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
    }

    if (err != 0)
      throw new MarlinIOException("Edit topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
  }

  @Override
  public void editTopic(String streamPath, String topicName,
                        TopicDescriptor topicDesc) throws IOException {

    maprfs.populateUserInfo(new Path(streamPath));

    int nfeeds = 0;
    MarlinTimestampType type = MarlinTimestampType.NoTimestampType;
    MTopicDescriptor mTopicDesc = (MTopicDescriptor)topicDesc;
    if (mTopicDesc.hasPartitions()) {
      TableDescriptorImpl desc = checkStreamAndGetTableDescriptor(streamPath);
      boolean isChangelog = desc.isChangelog();
      if (isChangelog) {
        throw new IllegalArgumentException("Changelog topic partition number cannot be changed");
      }
      nfeeds = mTopicDesc.getPartitions();

      if (nfeeds < 1) {
        throw new IllegalArgumentException("Number of partitions in a topic must be greater than or equal to 1");
      }

      if (nfeeds > mdef.getMaxFeedsPerTopic()) {
        throw new IllegalArgumentException("Number of partitions cannot be greater than " + mdef.getMaxFeedsPerTopic());
      }
    }

    if (mTopicDesc.hasTimestampType()) {
      type = timestampTypeToMarlinTimestampType(mTopicDesc.getTimestampType());
    }

    String topicFullName = streamPath + ':' + topicName;
    int err = EditTopic(_clntPtr, topicFullName, nfeeds, type.getNumber());

    if (err == Errno.ENOENT) {
      throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
    }

    if (err != 0)
      throw new MarlinIOException("Edit topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
  }

  @Override
  public void deleteTopic(String streamPath, String topicName)
      throws IOException {

    maprfs.populateUserInfo(new Path(streamPath));
    String topicFullName = streamPath + ':' + topicName;
    int err = DeleteTopic(_clntPtr, topicFullName);

    if (err == Errno.ENOENT) { 
      throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
    }

    if (err == Errno.EINVAL) {
      throw new UnknownTopicOrPartitionException("Topic name is illegal");
    }

    if (err != 0)
      throw new MarlinIOException("Delete topic failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
  }

  @Override
  public TopicDescriptor getTopicDescriptor(String streamPath, String topicName)
      throws IOException {
    checkStreamAndGetTableDescriptor(streamPath);
    maprfs.populateUserInfo(new Path(streamPath));
    String topicFullName = streamPath + ':' + topicName;
    MarlinTopicMetaEntry mentry = getTopicMetaEntry(topicFullName);
    MTopicDescriptor desc = new MTopicDescriptor();
    desc.setPartitions(mentry.getFeedIdsCount());
    desc.setTimestampType(marlinTimestampTypeToTimestampType(mentry.getTimestampType()));
    return desc;
  }

  public MarlinTopicMetaEntry getTopicMetaEntry(String topicFullName)
     throws IOException {

    maprfs.populateUserInfo(new Path(topicFullName));

    ErrorValue errVal = new ErrorValue();
    errVal.error = 0;
    byte[] entry = GetTopicMetaEntry(_clntPtr, topicFullName, errVal);

    if (errVal.error == Errno.ENOENT) {
      throw new NoSuchFileException("Topic " + topicFullName + " does not exist.");
    }

    if (errVal.error != 0) {
      int err = errVal.error;
      throw new MarlinIOException("GetTopicMetaEntry failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
    }

    MarlinTopicMetaEntry mentry = MarlinTopicMetaEntry.parseFrom(entry);
    return mentry;
  }

  private TopicFeedStatInfo statTopicFeed(String topicFullName,
                                          int feedId,
                                          boolean headOnly)
      throws IOException {

    maprfs.populateUserInfo(new Path(topicFullName));

    ErrorValue errVal = new ErrorValue();
    errVal.error = 0;

    byte[] reply = StatTopicFeed(_clntPtr, topicFullName,
                                 feedId,
                                 headOnly,
                                 errVal);
    if (errVal.error != 0) {
      throw new MarlinIOException("StatTopicFeed for topic " +
                            topicFullName +
                            " partitionId " + feedId +
                            " failed with error : " +
                            Errno.toString(errVal.error) +
                            "(" + errVal.error + ")").setErrorCode(errVal.error);
    }
    if (reply == null)
      throw new IOException("StatTopicFeed for topic " +
                            topicFullName +
                            " returned null");

    return TopicFeedStatInfo.parseFrom(reply);
  }

  private void statTabletsAndPopulateFeedInfo(final String topicFullName,
                                              List<TopicFeedInfo> feedList,
                                              final boolean headOnly)
      throws IOException {

    // Fire the stat calls
    LOG.debug("Begin : stat for all feeds");
    List<Future<TopicFeedStatInfo>> futureResponses =
      new ArrayList<Future<TopicFeedStatInfo>>();
    for (int i = 0; i < feedList.size(); ++i) {
      final int partitionId = i;
      futureResponses.add(topicFeedStatService.submit(new Callable<TopicFeedStatInfo>() {
        @Override
        public TopicFeedStatInfo call() {
          try {
            LOG.debug("StatTopicFeed topic " + topicFullName +
                      " partitionId " + partitionId);
            return statTopicFeed(topicFullName, partitionId, headOnly);
          } catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
          }
        }
      }));
    }

    // wait for the stat calls to finish
    int feedId = 0;
    for (Future<TopicFeedStatInfo> future : futureResponses) {
      TopicFeedStatInfo resp = null;
      try {
        resp = future.get();
        if (resp == null)
          throw new IOException("TopicFeedStat failed on one or more partitions");
      } catch (Exception e) {
        LOG.error(e.getMessage());
        throw new IOException(e.toString());
      }

      feedList.get(feedId).updateStat(resp);
      ++feedId;
    }
  }

  public List<TopicFeedInfo> infoTopicCommon(String topicFullName,
                                             boolean headOnly)
      throws IOException {

    maprfs.populateUserInfo(new Path(topicFullName));
    ErrorValue errVal = new ErrorValue();
    errVal.error = 0;
    byte[] entry = GetTopicMetaEntry(_clntPtr, topicFullName, errVal);
    if (errVal.error != 0) {
      int err = errVal.error;
      throw new MarlinIOException("GetTopicMetaEntry failed with error : " +
                            Errno.toString(err) + " (" + err + ")").setErrorCode(err);
    }

    LOG.debug("Begin : get topic meta");
    MarlinTopicMetaEntry tmeta = MarlinTopicMetaEntry.parseFrom(entry);
    if (tmeta.getIsDeleted())
      throw new IOException("Topic " + topicFullName + " is already deleted");
    LOG.debug("Done : get topic meta");

    Map<String, MarlinTopicMetaEntry> topicMetaMap =
      new HashMap<String, MarlinTopicMetaEntry>();
    topicMetaMap.put(topicFullName, tmeta);

    List<TopicFeedInfo> statList = new ArrayList<TopicFeedInfo>();
    for (int feedId : tmeta.getFeedIdsList()) {
     LcPartitionStatus lcStatus = null;
     lcStatus = tmeta.getLcProgress().getTopicFeedLcStatus(feedId);
     statList.add(new TopicFeedInfo(feedId, lcStatus));
    }

    statTabletsAndPopulateFeedInfo(topicFullName, statList, headOnly);

    LOG.debug("Begin : get cursors");
    String[] fullPathSplits  = topicFullName.split(":", 2);
    List<CursorInfo> cursorList = scanCursors(fullPathSplits[0],
                                              null /*listenerGID*/,
                                              fullPathSplits[1],
                                              -1 /*feedId*/,
                                              topicMetaMap);
    for (CursorInfo ci : cursorList) {
      int feedId = ci.feedId();

      if (feedId >= statList.size())
        continue;

      statList.get(feedId).addCursor(ci);
    }
    LOG.debug("Done : get cursors");

    return statList;
  }

  public List<TopicFeedInfo> infoTopic(String topicFullName)
      throws IOException {
    return infoTopicCommon(topicFullName, false /*headOnly*/);
  }

  private MarlinTopicMetaEntry jsonRecToTopicMeta(Document rec) {
    LOG.debug("Key:: {} doc:: {}", rec.getIdString(), rec);

    MarlinTopicMetaEntry.Builder tm = MarlinTopicMetaEntry.newBuilder();
    Boolean bv;
    Long lv;

    lv = rec.getLongObj(mdef.getCfTopicMeta() + '.' +
                        mdef.getFTopicUpdateSeq());
    if (lv != null)
     tm.setUpdateSeq(lv);   

    bv = rec.getBooleanObj(mdef.getCfTopicMeta() + '.' +
                           mdef.getFTopicIsDeleted());
    if (bv != null)
      tm.setIsDeleted(bv);

    lv = rec.getLongObj(mdef.getCfTopicMeta() + '.' +
                        mdef.getFTopicUniq());
    if (lv != null) {
      long ltopicUniq = lv;
      tm.setTopicUniq((int)ltopicUniq);   
    }

    List<Object> feedIds = rec.getList(mdef.getCfTopicMeta() + '.' +
                                       mdef.getFTopicFeedIds());
    for (Object obj : feedIds) {
      long feedLongId = (Long)obj;
      int feedId = (int)feedLongId;
      tm.addFeedIds(feedId);
    }

    return tm.build();
  }

  private String printableFid(FidMsg fid) {
    return "" + fid.getCid() + "." +
                fid.getCinum() + "." +
                fid.getUniq();
  }

  private TopicFeedStatResponse statFeedsOnTablet(String streamName,
                                                  FidMsg fid)
      throws IOException {

    MapRUserInfo userInfo = maprfs.populateAndGetUserInfo(new Path(streamName));
    ErrorValue errVal = new ErrorValue();
    errVal.error = 0;

    byte[] reply = StatFeedsOnTablet(_clntPtr, streamName,
                                     fid.getCid(),
                                     fid.getCinum(),
                                     fid.getUniq(),
                                     errVal,
                                     userInfo);
    if (errVal.error != 0) {
      throw new MarlinIOException("StatFeedsOnTablet for tablet " +
                            printableFid(fid) +
                            " failed with error : " +
                            Errno.toString(errVal.error) +
                            "(" + errVal.error + ")").setErrorCode(errVal.error);
    }
    if (reply == null)
      throw new IOException("StatFeedsOnTablet for tablet " +
                            printableFid(fid) + " returned null");

    return TopicFeedStatResponse.parseFrom(reply);
  }

  private List<FidMsg> getFeedTabletFids(final String streamName)
      throws IOException {

    MapRTabletScanner scanner = maprfs.getTabletScanner(new Path(streamName), null);
    List<FidMsg> fidList = new ArrayList<FidMsg> ();
    boolean isFirst = true;

    List<TabletDesc> nextSet;
    while ((nextSet = scanner.nextSet()) != null) {
      for (TabletDesc tablet: nextSet) {
        if (isFirst) {
          // the first tablet is a meta tablet, skip it.
          isFirst = false;
          continue;
        }
        fidList.add(tablet.getFid());
      }
    }
    return fidList;
  }

  private void scanAllTabletsAndPopulateFeedInfo(final String streamName,
                                                 Map<String, MarlinTopicMetaEntry> topicMetaMap,
                                                 Map<String, List<TopicFeedInfo>> topicStatMap)
      throws IOException {

    LOG.debug("Begin : Fetch list of tablets");
    List<FidMsg> tabletFids = getFeedTabletFids(streamName);
    LOG.debug("Done : Fetch list of tablets");

    // Fire the stat calls
    LOG.debug("Begin : stat for all tablets");
    List<Future<TopicFeedStatResponse>> futureResponses =
      new ArrayList<Future<TopicFeedStatResponse>>();
    for (final FidMsg fid : tabletFids) {
      futureResponses.add(topicFeedStatService.submit(new Callable<TopicFeedStatResponse>() {
        @Override
        public TopicFeedStatResponse call() {
          try {
            LOG.debug("StatFeedsOnTablet on tablet " + printableFid(fid));
            return statFeedsOnTablet(streamName, fid);
          } catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
          }
        }
      }));
    }

    // wait for the stat calls to finish
    for (Future<TopicFeedStatResponse> future : futureResponses) {
      TopicFeedStatResponse resp = null;
      try {
        resp = future.get();
        if (resp == null)
          throw new IOException("TopicFeedStat failed on one or more tablets");
      } catch (Exception e) {
        LOG.error(e.getMessage());
        throw new IOException(e.toString());
      }

      for (TopicFeedStatInfo stat : resp.getFeedInfosList()) {
        String seqPrefix = stat.getSeqPrefix().substring(1);

        Document rdoc = MarlinRowKeyDecoder.decodeTopicFeedKey(seqPrefix);
        String topic = rdoc.getString(MarlinRowKeyDecoder.TOPIC);
        int feedId = rdoc.getInt(MarlinRowKeyDecoder.PARTITION);
        int uniq = rdoc.getInt(MarlinRowKeyDecoder.TOPIC_UNIQ);
        String topicFullName = streamName + ":" + topic;

        MarlinTopicMetaEntry tmeta = topicMetaMap.get(topicFullName);
        if (tmeta == null) {
          LOG.debug("missing entry in topicMetaMap for topic " + topic);
          continue;
        }
        if (tmeta.getTopicUniq() != uniq) {
          LOG.debug("mismatch in topicUniq for topic " + topic);
          continue;
        }

        List<TopicFeedInfo> feedList = topicStatMap.get(topic);
        if (feedList == null || feedId >= feedList.size())
          continue;

        feedList.get(feedId).updateStat(stat);
      }
    }
    LOG.debug("Done : stat for all tablets");
  }

  @Override
  public List<String> listTopics(String streamName)
      throws IOException, IllegalArgumentException {
    Map<String, List<TopicFeedInfo>> topicsMap = listTopicsForStream(streamName);
    List topicNamesList = new ArrayList<String>();

    for (String topicName : topicsMap.keySet()) {
      topicNamesList.add(topicName);
    }
    return topicNamesList;
  }

  public Map<String, List<TopicFeedInfo>> listTopicsForStream(String streamName)
      throws IOException, IllegalArgumentException {
    checkStreamAndGetTableDescriptor(streamName);

    Map<String, List<TopicFeedInfo>> topicStatMap;
    topicStatMap = new HashMap<String, List<TopicFeedInfo>>();

    QueryCondition condition = MapRDBImpl.newCondition()
        .and()
          .is(ID_FIELD, GREATER, mdef.getKeyPrefixTopicMeta())
          .is(ID_FIELD, LESS, mdef.getKeyPrefixTopicMetaEnd())
        .close()
    .build();

    try (Table table = MapRDBImpl.getTable(new Path(streamName));
        DocumentStream recStream = table.find(condition, mdef.getCfTopicMeta())) {

      Map<String, MarlinTopicMetaEntry> topicMetaMap =
          new HashMap<String, MarlinTopicMetaEntry>();

      // get all topic names from meta-tablet
      LOG.debug("Begin : get topic names");
      Iterator<Document> iter = recStream.iterator();
      while (iter.hasNext()) {
        Document rec = iter.next();
        MarlinTopicMetaEntry tmeta = jsonRecToTopicMeta(rec);
        if (tmeta.getIsDeleted())
          continue;

        String rowKey = rec.getIdString();
        String topicName = rowKey.substring(1); // tXX
        String topicFullName = streamName + ":" + topicName;

        topicMetaMap.put(topicFullName, tmeta);
        List<TopicFeedInfo> fstatList = new ArrayList<TopicFeedInfo>();
        for (int feedId : tmeta.getFeedIdsList())
          fstatList.add(new TopicFeedInfo(feedId));
        topicStatMap.put(topicName, fstatList);
      }
      LOG.debug("Done : get topic names");

      // scan tablets and populate feed info.
      scanAllTabletsAndPopulateFeedInfo(streamName, topicMetaMap, topicStatMap);

      // get all cursors from the meta-tablet
      LOG.debug("Begin : get cursors");
      List<CursorInfo> cursorList = scanCursors(streamName,
          null /*listenerGID*/,
          null /*topicName*/,
          -1 /*feedId*/,
          topicMetaMap);
      for (CursorInfo ci : cursorList) {
        int feedId = ci.feedId();

        List<TopicFeedInfo> feedList = topicStatMap.get(ci.topic());
        if (feedList == null || feedId >= feedList.size())
          continue;

        feedList.get(feedId).addCursor(ci);
      }
      LOG.debug("Done : get cursors");
    }

    return topicStatMap;
  }

  @Override
  public Collection<Object> listConsumerGroups(String streamName)
          throws IOException, IllegalArgumentException {
    List<String> consumerGroups = new ArrayList<>();
    List<Object> consumerGroupListings = new ArrayList<>();

    for (AssignInfo ai : listAssigns(streamName, null, null)) {
      if (!consumerGroups.contains(ai.listenerID)) {
        consumerGroups.add(ai.listenerID);
        consumerGroupListings.add(new ConsumerGroupListing(ai.listenerID, true));
      }
    }

    return consumerGroupListings;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(
      String streamName, String groupId)
      throws IOException, IllegalArgumentException {
    Map<TopicPartition, OffsetAndMetadata> groupOffsetListing = new HashMap<>();

    List<CursorInfo> cursorInfos = listCursors(streamName, groupId, null, -1);
    for (CursorInfo ci: cursorInfos) {
      TopicPartition tp =
          new TopicPartition(ci.streamName() + ":" + ci.topic(), ci.feedId());
      OffsetAndMetadata om = new OffsetAndMetadata(ci.cursor());
      groupOffsetListing.put(tp, om);
    }

    return groupOffsetListing;
  }

  @Override
  public Map<TopicPartition, Errors> alterConsumerGroupOffsets(String stream,
      String groupId, Map<TopicPartition, OffsetAndMetadata> offsets)
      throws IOException, IllegalArgumentException {
    Map<TopicPartition, Errors> topicPartitionErrors = new HashMap<>();

    for (Map.Entry<TopicPartition, OffsetAndMetadata> tpOfsset : offsets.entrySet()) {
      long offset = tpOfsset.getValue().offset();

      String streamPath = null;
      String topicName = null;

      String[] tokens = tpOfsset.getKey().topic().split(":");

      if (tokens.length == 2) {
        streamPath = tokens[0];
        topicName = tokens[1];
      } else if (stream != null) {
        streamPath = stream;
        topicName = tokens[0];
      }

      if (streamPath != null) {
        int partition = tpOfsset.getKey().partition();
        alterCursors(streamPath, groupId, topicName, partition, offset);

        topicPartitionErrors.put(tpOfsset.getKey(), Errors.NONE);
      } else {
        topicPartitionErrors.put(tpOfsset.getKey(),
            Errors.forException(new KafkaException("No default stream name specified in the configuration options")));
      }
    }

    return topicPartitionErrors;
  }

  private DocumentStream GetCursorList(Table table, String listenerGID,
                                      String topic, String feedId) throws IOException
  {
    String regex = ".*" + topic + ".*" + feedId + ".*" + listenerGID + ".*";

    QueryCondition condition = MapRDBImpl.newCondition()
        .and()
          .is(ID_FIELD, GREATER, mdef.getKeyCursorPrefix())
          .is(ID_FIELD, LESS, mdef.getKeyCursorPrefixEnd())
          .matches(ID_KEY, regex)
        .close()
    .build();

    return table.find(condition, mdef.getCfCursors());
  }

  private boolean FilterResult(Document rec, String streamName,
                               String listenerGID,
                               String topic, int feedId, CursorInfo ci,
                               Map<String, MarlinTopicMetaEntry> topicMetaMap)
     throws IOException {

    Document rowKeyDoc = MarlinRowKeyDecoder.decodeCursorKey(rec.getIdString());

    String rtopic = rowKeyDoc.getString(MarlinRowKeyDecoder.TOPIC);
    if (topic != null && !rtopic.equals(topic)) {
      return false;
    }

    Integer topicUniq = rowKeyDoc.getInt(MarlinRowKeyDecoder.TOPIC_UNIQ);

    String topicFullName = streamName + ":" + rtopic;

    MarlinTopicMetaEntry tmeta = topicMetaMap.get(topicFullName);
    if (tmeta == null) {
      ErrorValue errVal = new ErrorValue();
      byte[] entry = GetTopicMetaEntry(_clntPtr, topicFullName, errVal);
      if (errVal.error != 0)
        return false;

      tmeta = MarlinTopicMetaEntry.parseFrom(entry);
      topicMetaMap.put(topicFullName, tmeta);
    }

    if (tmeta.getIsDeleted()) {
      return false;
    }

    if (tmeta.getTopicUniq() != topicUniq) {
      return false;
    }

    Integer rfeedId = rowKeyDoc.getInt(MarlinRowKeyDecoder.PARTITION);
    if (feedId >= 0 && feedId != rfeedId) {
      return false;
    }

    String rlGId = rowKeyDoc.getString(MarlinRowKeyDecoder.CONSUMER_GROUP);
    if (listenerGID != null && !listenerGID.equals(rlGId)) {
      return false;
    }

    /* Read the actual cursor value from the result */
    Long cursor = rec.getLongObj(mdef.getCfCursors() + '.' +
                                 mdef.getFCursor());
    if (cursor == null) {
      throw new IOException("invalid cursor value");
    }

    Long timestamp = rec.getLongObj(mdef.getCfCursors() + '.' +
                                    mdef.getFTimestamp());
    if (timestamp == null) {
      throw new IOException("invalid timestamp value");
    }

    ci.Init(streamName, rtopic, rlGId, rfeedId,
            cursor.longValue(), timestamp.longValue());
    return true;
  }

  public List<CursorInfo> alterCursors(String streamName, String listenerGID,
                                       String topicName, int feedId, long offset)
          throws IOException {
    // filter and alter the cursor rows
    Map<String, MarlinTopicMetaEntry> topicMetaMap =
            new HashMap<String, MarlinTopicMetaEntry>();
    List<CursorInfo> ciList = updateCursor(streamName,
            listenerGID, topicName, feedId, offset,
            topicMetaMap);

    // populate feed infos
    Map<String, List<TopicFeedInfo>> topicMap =
            new HashMap<String, List<TopicFeedInfo>>();
    List<CursorInfo> cursorList = new ArrayList<CursorInfo>();
    for (CursorInfo ci : ciList) {
      String topicFullName = ci.streamName() + ":" + ci.topic();

      MarlinTopicMetaEntry tmeta = topicMetaMap.get(topicFullName);
      if (tmeta == null)
        continue;

      List<TopicFeedInfo> sinfoList = topicMap.get(ci.topic());
      if (sinfoList == null) {
        sinfoList = new ArrayList<TopicFeedInfo>();
        for (int idx : tmeta.getFeedIdsList())
          sinfoList.add(new TopicFeedInfo(idx));

        statTabletsAndPopulateFeedInfo(topicFullName, sinfoList,
                true /*headOnly*/);
        topicMap.put(ci.topic(), sinfoList);
      }

      TopicFeedInfo sinfo = sinfoList.get(ci.feedId());
      ci.setTopicFeedInfo(sinfo);
      cursorList.add(ci);
    }

    return cursorList;
  }

  public List<CursorInfo> listCursors(String streamName, String listenerGID,
                                      String topicName, int feedId)
      throws IOException {

    if (listenerGID == null && topicName == null && (feedId < 0)) {
      // fast-path for all cursors
      return listAllCursors(streamName);
    } else if (listenerGID == null && (feedId < 0)) {
      // fast-path for all cursors of a topic
      return listCursorsForTopic(streamName + ":" + topicName);
    }

    // filter and get the cursor rows
    Map<String, MarlinTopicMetaEntry> topicMetaMap =
      new HashMap<String, MarlinTopicMetaEntry>();
    List<CursorInfo> ciList =  scanCursors(streamName,
                                           listenerGID, topicName, feedId,
                                           topicMetaMap);

    // populate feed infos
    Map<String, List<TopicFeedInfo>> topicMap =
      new HashMap<String, List<TopicFeedInfo>>();
    List<CursorInfo> cursorList = new ArrayList<CursorInfo>();
    for (CursorInfo ci : ciList) {
      String topicFullName = ci.streamName() + ":" + ci.topic();

      MarlinTopicMetaEntry tmeta = topicMetaMap.get(topicFullName);
      if (tmeta == null)
        continue;

      List<TopicFeedInfo> sinfoList = topicMap.get(ci.topic());
      if (sinfoList == null) {
        sinfoList = new ArrayList<TopicFeedInfo>();
        for (int idx : tmeta.getFeedIdsList())
          sinfoList.add(new TopicFeedInfo(idx));

        statTabletsAndPopulateFeedInfo(topicFullName, sinfoList,
                                       true /*headOnly*/);
        topicMap.put(ci.topic(), sinfoList);
      }

      TopicFeedInfo sinfo = sinfoList.get(ci.feedId());
      ci.setTopicFeedInfo(sinfo);
      cursorList.add(ci);
    }

    return cursorList;
  }

  public List<CursorInfo> listAllCursors(String streamName)
      throws IOException {

    // This stats the feeds and gets the cursors for all feeds.
    Map<String, List<TopicFeedInfo>> tfMap = listTopicsForStream(streamName);

    List<CursorInfo> cursorList = new ArrayList<CursorInfo>();
    for (List<TopicFeedInfo> feedInfoList : tfMap.values()) {
      for (TopicFeedInfo feedInfo : feedInfoList) {
        for (CursorInfo ci : feedInfo.cursorList()) {
          ci.setTopicFeedInfo(feedInfo);
          cursorList.add(ci);
        }
      }
    }
    return cursorList;
  }

  public List<CursorInfo> listCursorsForTopic(String topicFullName)
      throws IOException {

    // This stats the feeds and gets the cursors for all feeds of topic.
    List<TopicFeedInfo> feedList = infoTopicCommon(topicFullName,
                                                   true /*headOnly*/);

    List<CursorInfo> cursorList = new ArrayList<CursorInfo>();
    for (TopicFeedInfo feedInfo : feedList) {
      for (CursorInfo ci : feedInfo.cursorList()) {
        ci.setTopicFeedInfo(feedInfo);
        cursorList.add(ci);
      }
    }
    return cursorList;
  }

  private List<CursorInfo> scanCursors(String streamName, String listenerGID,
                                       String topicName, int feedId,
                                       Map<String, MarlinTopicMetaEntry> topicMetaMap) throws IOException {

    maprfs.populateUserInfo(new Path(streamName));
    List<CursorInfo> cursorList = new ArrayList<CursorInfo>();
    try (Table table = MapRDBImpl.getTable(new Path(streamName));
        DocumentStream stream = GetCursorList(table,
            listenerGID == null ? "" : listenerGID,
            topicName == null ?  "" : topicName,
            feedId >= 0 ?
                Integer.toString(feedId, 16) : "")) {

      Iterator<Document> iter = stream.iterator();
      while (iter.hasNext()) {
        Document rec = iter.next();
        CursorInfo ci = new CursorInfo();
        boolean useRecord = FilterResult(rec, streamName, listenerGID,
            topicName, feedId, ci,
            topicMetaMap);
        if (useRecord) {
          cursorList.add(ci);
        }
      }
    }

    return cursorList;
  }

  public void deleteCursors(String streamName,
                            String listenerGID,
                            String topicName,
                            int feedId) throws IOException {

    maprfs.populateUserInfo(new Path(streamName));

    try (Table table = MapRDBImpl.getTable(new Path(streamName));
        DocumentStream stream = GetCursorList(table,
            listenerGID == null ? "" : listenerGID,
            topicName == null ?  "" : topicName,
            feedId >= 0 ?
                Integer.toString(feedId, 16) : "")) {

      Map<String, MarlinTopicMetaEntry> topicMetaMap = new HashMap<String, MarlinTopicMetaEntry>();
      Iterator<Document> iter = stream.iterator();
      while (iter.hasNext()) {
        Document rec = iter.next();
        CursorInfo ci = new CursorInfo();
        boolean useRecord = FilterResult(rec, streamName, listenerGID, topicName,
            feedId, ci, topicMetaMap);
        if (useRecord) {
          String key = rec.getIdString();
          table.delete(key);
        }
      }
    }
  }

  private List<CursorInfo> updateCursor(String streamName, String listenerGID,
                                        String topicName, int feedId, long offset,
                                        Map<String, MarlinTopicMetaEntry> topicMetaMap) throws IOException {

    maprfs.populateUserInfo(new Path(streamName));
    List<CursorInfo> cursorList = new ArrayList<CursorInfo>();

    try (Table table = MapRDBImpl.getTable(new Path(streamName));
        DocumentStream stream = GetCursorList(table,
            listenerGID == null ? "" : listenerGID,
            topicName == null ?  "" : topicName,
            feedId >= 0 ?
                Integer.toString(feedId, 16) : "")) {

      Iterator<Document> iter = stream.iterator();
      while (iter.hasNext()) {
        Document rec = iter.next();

        /* update document corresponding to cursor */
        DocumentMutation m = MapRDBImpl.newMutation()
            /* field 'c' corresponds to the cursor field 'committedoffset' */
            .set("cursors.c", offset);
        table.update(rec.getIdString(), m);

        CursorInfo ci = new CursorInfo();
        boolean useRecord = FilterResult(rec, streamName, listenerGID,
            topicName, feedId, ci,
            topicMetaMap);

        if (useRecord) {
          cursorList.add(ci);
        }
      }
    }

    return cursorList;
  }

  private DocumentStream GetAssignList(Table table, String listenerGID,
                                                   String topic) throws IOException
  {
    String regex = ".*" + topic + ".*" + listenerGID + ".*";

    QueryCondition condition = MapRDBImpl.newCondition()
        .and()
          .is(ID_FIELD, GREATER, mdef.getKeyAssignPrefix())
          .is(ID_FIELD, LESS, mdef.getKeyAssignPrefixEnd())
          .matches(ID_KEY, regex)
        .close()
    .build();

    return table.find(condition, mdef.getCfFeedAssigns());
  }

  public boolean FilterResult(Document rec, String streamName,
                              String listenerGID,
                              String topic,
                              AssignInfo ai,
                              Map<String, MarlinTopicMetaEntry> topicMetaMap)
       throws IOException {

    Document rowKeyDoc = MarlinRowKeyDecoder.decodeAssignKey(rec.getIdString());

    String rtopic = rowKeyDoc.getString(MarlinRowKeyDecoder.TOPIC);
    if (topic != null && !rtopic.equals(topic)) {
      return false;
    }
    Integer topicUniq = rowKeyDoc.getInt(MarlinRowKeyDecoder.TOPIC_UNIQ);

    String topicFullName = streamName + ":" + rtopic;
    MarlinTopicMetaEntry tmeta = topicMetaMap.get(topicFullName);
    if (tmeta == null) {
      ErrorValue errVal = new ErrorValue();
      byte[] entry = GetTopicMetaEntry(_clntPtr, topicFullName, errVal);
      if (errVal.error != 0) {
        return false;
      }

      tmeta = MarlinTopicMetaEntry.parseFrom(entry);
      topicMetaMap.put(topicFullName, tmeta);
    }

    if (tmeta.getIsDeleted()) {
      return false;
    }

    if (tmeta.getTopicUniq() != topicUniq) {
      return false;
    }

    String rlGId = rowKeyDoc.getString(MarlinRowKeyDecoder.CONSUMER_GROUP);
    if (listenerGID != null && !listenerGID.equals(rlGId)) {
      return false;
    }

    Long assignSeqNum = rec.getLongObj(mdef.getCfFeedAssigns() + '.' +
                                       mdef.getFAssignSeqNum());

    Long numFeeds = rec.getLongObj(mdef.getCfFeedAssigns() + '.' +
                                   mdef.getFAssignFeeds());

    String[] listeners = new String[(int)numFeeds.longValue()];
    int numListeners = 0;
    List<List<Integer>> listenerAssignment = new
      ArrayList<List<Integer>>();

    for (int i = 0; i < (int)numFeeds.longValue(); i++) {
      String feedStr = String.format("%s%03x", mdef.getKeyPrefixFeedId(), i);
      String assignment = rec.getString(mdef.getCfFeedAssigns() + '.' + feedStr);
      if (assignment == null) {
        continue;
      }
      
      boolean listenerFound = false;
      for (int j = 0; j < numListeners; j++) {
        if (listeners[j].equals(assignment)) {
          listenerFound = true;
          List<Integer> assignments = listenerAssignment.get(j);
          assignments.add(i);
          break;
        }
      }

      if (!listenerFound) {
        listeners[numListeners++] = assignment;
        ArrayList<Integer> assignments = new  ArrayList<Integer>();
        listenerAssignment.add(assignments);
        assignments.add(i);
      }
    }


    ai.Init(streamName, rtopic, rlGId, assignSeqNum.longValue(), listeners,
            numListeners,
            listenerAssignment);

    return true;
  }
  
  public List<AssignInfo> listAssigns(String streamName, String listenerGID,
                                      String topicName) throws IOException {
    List<AssignInfo> assignList = new ArrayList<AssignInfo>();

    maprfs.populateUserInfo(new Path(streamName));

    try (Table table = MapRDBImpl.getTable(new Path(streamName));
        DocumentStream stream = GetAssignList(table,
            listenerGID == null ? "" : listenerGID,
            topicName == null ?  "" : topicName)) {

      Map<String, MarlinTopicMetaEntry> topicMetaMap =
          new HashMap<String, MarlinTopicMetaEntry>();

      Iterator<Document> iter = stream.iterator();
      while (iter.hasNext()) {
        Document rec = iter.next();
        AssignInfo ai = new AssignInfo();
        boolean useRecord = FilterResult(rec, streamName, listenerGID, topicName,
            ai, topicMetaMap);
        if (useRecord) {
          assignList.add(ai);
        }
      }
    }

    return assignList;
  }

  public void close() {
    close(60, TimeUnit.SECONDS);
  }

  @Deprecated
  public void close(long duration, TimeUnit unit) {
    close(Duration.ofMillis(unit.toMillis(duration)));
  }

  public void close(Duration timeout) {
    CloseAdmin(_clntPtr);
    _clntPtr = 0;

    synchronized(MarlinAdminImpl.class) {
      numAdmins--;
      if (numAdmins == 0) {
        try {
          topicFeedStatService.shutdown();
          if (!topicFeedStatService.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            topicFeedStatService.shutdownNow();
          }
        } catch (Exception e) {
          topicFeedStatService.shutdownNow();
        } finally {
          topicFeedStatService = null;
        }
      }
    }

  }
}
