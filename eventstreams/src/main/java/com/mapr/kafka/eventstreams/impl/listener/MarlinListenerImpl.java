package com.mapr.kafka.eventstreams.impl.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.baseutils.Errno;
import com.mapr.kafka.eventstreams.impl.MarlinClient;
import com.mapr.kafka.eventstreams.impl.MarlinTopicInfo;
import com.mapr.fs.jni.MapRUserInfo;
import com.mapr.fs.jni.MarlinJniListener;
import com.mapr.fs.jni.NativeData;
import com.mapr.fs.MapRFileSystem;

import com.mapr.fs.proto.Dbserver.CDCOpenFormatType;
import com.mapr.fs.proto.Marlinserver.MarlinConfigDefaults;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;
import com.mapr.fs.proto.Marlinserver.JoinGroupDesc;
import com.mapr.fs.proto.Marlinserver.JoinGroupInfo;
import com.mapr.fs.proto.Marlinserver.JoinGroupResponse;
import com.mapr.fs.ShimLoader;
import com.mapr.kafka.eventstreams.TopicRefreshRegexListener;
import com.mapr.kafka.eventstreams.TopicRefreshListListener;
import com.mapr.kafka.eventstreams.impl.listener.MarlinListener.MarlinJoinCallback;

/**
 * A Marlin wrapper that provides all the Kafka Consumer API
 */
public class MarlinListenerImpl extends MarlinJniListener {

    static {
      ShimLoader.load();
    }

    private static final Logger LOG = LoggerFactory.getLogger(MarlinListenerImpl.class);
    private static final Integer DEFAULT_MAX_MSGS_BYTES = 50 * 1024 * 1024; // MaxMsgBytes
    // TODO Match this kafka's implementation for offsetsForTimes API.
    public static final long EARLIEST_TIMESTAMP = 0L;

    private boolean closed = false;
    protected String defaultStreamName;
    private boolean recordStripStreamPath = false;
    protected int rpcTimeoutMs = 0;
    private ConsumerInterceptors<?, ?> interceptors;
    private boolean metricsEnabled;
    private Metrics metrics;
    private Fetcher.FetchManagerMetrics sensors;
    protected final MarlinInternalDefaults marlinInternalDefaults =
                                MarlinInternalDefaults.getDefaultInstance();

    protected MarlinCommitCallbackWrapperImpl autoCommitCbWrapper = null;
    private final CDCOpenFormatType _cdcOFType = CDCOpenFormatType.COFT_NONE;

    public CDCOpenFormatType getCDCOpenFormatType() {
      return _cdcOFType;
    }

    protected void checkConsumerConfig(ConsumerConfig config) throws KafkaException {
      MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

      // Just reading the configuration should check that the client.id
      // exists.  If it does not exist, then ConsumerConfig throws not
      // found exception.
      try {
        String clientID = config.getString(mConfDef.getClientID());
      } catch (ConfigException e) {
        LOG.error("Invalid client id configuration");
        throw e;
      }

      try {
        String groupID = config.getString(mConfDef.getGroupID());
      } catch (ConfigException e) {
        LOG.error("Invalid group.id configuration");
        throw e;
      }

      try {
        boolean autoCommitEnable =
          config.getBoolean(mConfDef.getAutoCommitEnabled());
      } catch (ConfigException e) {
        LOG.error("Invalid auto commit enabled configuration");
        throw e;
      }

      try {
        Long autoCommitInt =
          config.getLong(mConfDef.getAutoCommitInterval());
        if (autoCommitInt < 0) {
          throw new ConfigException(mConfDef.getAutoCommitInterval() + " cannot be negative number");
        }
      } catch (ConfigException e) {
        LOG.error(mConfDef.getAutoCommitInterval() + " configuration");
        throw e;
      }

      try {
        Long metadataMaxAge =
          config.getLong(mConfDef.getMetadataMaxAge());
        if (metadataMaxAge < 0) {
          throw new ConfigException(mConfDef.getMetadataMaxAge() + " cannot be negative number");
        }
      } catch (ConfigException e) {
        LOG.error(mConfDef.getMetadataMaxAge() + " configuration");
        throw e;
      }

      try {
        Integer fetchMsgMaxBytes =
          config.getInt(mConfDef.getFetchMsgMaxBytesPerPartition());
        if (fetchMsgMaxBytes < 0) {
          throw new ConfigException(mConfDef.getFetchMsgMaxBytesPerPartition() + " cannot be negative number");
        }
      } catch (ConfigException e) {
        LOG.error(mConfDef.getFetchMsgMaxBytesPerPartition() + " configuration");
        throw e;
      }

      try {
        Integer fetchMinBytes =
          config.getInt(mConfDef.getFetchMinBytes());
        if (fetchMinBytes < 0) {
          throw new ConfigException(mConfDef.getFetchMinBytes() + " cannot be negative number");
        }
      } catch (ConfigException e) {
        LOG.error(mConfDef.getFetchMinBytes() + " configuration");
        throw e;
      }

      try {
        /* just fetching the value shud validate it */
        String autoOffsetReset =
          config.getString(mConfDef.getAutoOffsetReset());
      } catch (ConfigException e) {
        LOG.error(mConfDef.getAutoOffsetReset() + " configuration");
        throw e;
      }

      try {
        boolean stripStreamPath =
          config.getBoolean(mConfDef.getRecordStripStreamPath());
      } catch (ConfigException e) {
        LOG.error(mConfDef.getRecordStripStreamPath() + " configuration");
        throw e;
      }

    }

    protected void createJniListener(ConsumerConfig config,
                                     boolean hardMount,
                                     MapRUserInfo userInfo,
                                     CDCOpenFormatType cdcOFType) throws NetworkException {

      MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

      _clntPtr = OpenListener(config.getString(mConfDef.getClientID()),
                              config.getString(mConfDef.getGroupID()),
                              this.rpcTimeoutMs,
                              hardMount,
                              config.getBoolean(mConfDef.getAutoCommitEnabled()),
                              config.getLong(mConfDef.getAutoCommitInterval()),
                              null /* autoCommitCb, applicable for V10 */,
                              config.getLong(mConfDef.getMetadataMaxAge()),
                              config.getInt(mConfDef.getFetchMsgMaxBytesPerPartition()),
                              DEFAULT_MAX_MSGS_BYTES,
                              config.getInt(mConfDef.getFetchMinBytes()),
                              config.getInt(mConfDef.getFetchMaxWaitMs()),
                              config.getString(mConfDef.getAutoOffsetReset()),
                              defaultStreamName,
                              config.getLong(mConfDef.getConsumerBufferMemory()),
                              config.getBoolean(mConfDef.getNegativeOffsetOnEof()),
                              userInfo,
                              cdcOFType.ordinal(),
                              Integer.MAX_VALUE);
      if (_clntPtr == 0) {
        /**
         * <MAPR_ERROR>
         *   Unable to create MapRClient object for the cluster
         *   specified in hostname:port
         * </MAPR_ERROR>
         */
        throw new NetworkException("Could not create Consumer. Please ensure"
                                   + " that the CLDB service is configured"
                                   + " properly and is available");
      }
    }

    public MarlinListenerImpl(ConsumerConfig config,
                         ConsumerInterceptors<?, ?> ints,
                         CDCOpenFormatType cdcOFType) {
      LOG.debug("Starting Streams Listener with cdcOFType " + cdcOFType + ", ordinal " + cdcOFType.ordinal());

      this.interceptors = ints;

      autoCommitCbWrapper = (interceptors == null) ? null :
			 new MarlinCommitCallbackWrapperImpl(null /*callback */, interceptors);

      checkConsumerConfig(config);
      MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

      this.metricsEnabled = config.getBoolean(ConsumerConfig.METRICS_ENABLED_CONFIG);
      if (metricsEnabled) {
        this.metrics = buildMetrics(config);
        this.sensors = buildSensors(metrics);
      }

      this.defaultStreamName = null;
      try {
        this.defaultStreamName = config.getString(mConfDef.getConsumerDefaultStream());
        this.recordStripStreamPath = config.getBoolean(mConfDef.getRecordStripStreamPath());
      } catch (ConfigException e) {
        // It is okay for the default stream name to be not set.
        // So, just ignore the exception here.
      }

      boolean hardMount = true;
      try {
        this.rpcTimeoutMs = config.getInt(mConfDef.getRpcTimeout());
        hardMount = config.getBoolean(mConfDef.getHardMount());
      } catch (ConfigException e) {
        // It is okay for the default stream name or timeout to be not set.
        // So, just ignore the exception here.
      }

      MapRUserInfo userInfo;
      try {
        userInfo = MapRFileSystem.CurrentUserInfo();
      } catch (IOException e) {
        throw new KafkaException("Could not create MarlinListener", e);
      }

      createJniListener(config, hardMount, userInfo, cdcOFType);

      LOG.debug("Streams listener created");
    }

    private static Metrics buildMetrics(ConsumerConfig config){
      String clientId = config.getString(MarlinConfigDefaults.getDefaultInstance().getClientID());
      Map<String, String> metricsTags = Collections.singletonMap("client-id", clientId);
      MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
              .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
              .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
              .tags(metricsTags);
      List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
              MetricsReporter.class, Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
      JmxReporter jmxReporter = new JmxReporter();
      jmxReporter.configure(config.originals());
      reporters.add(jmxReporter);
      MetricsContext metricsContext = new KafkaMetricsContext("kafka.consumer",
              config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
      return new Metrics(metricConfig, reporters, Time.SYSTEM, metricsContext);
    }

    private static Fetcher.FetchManagerMetrics buildSensors(Metrics metrics) {
      FetcherMetricsRegistry registry = new FetcherMetricsRegistry(Collections.singleton("client-id"), "consumer");
      Fetcher.FetchManagerMetrics sensors = new Fetcher.FetchManagerMetrics(metrics, registry);

      //remove unsupported metrics from sensors we use
      metrics.removeMetric(metrics.metricInstance(registry.recordsPerRequestAvg));

      return sensors;
    }


  public Map<MetricName, ? extends Metric>  metrics(){
    return metricsEnabled ? metrics.metrics() : Collections.EMPTY_MAP;
  }

  public Set<TopicPartition> paused() {
      NativeData native_response = new NativeData();
      int err = Paused(_clntPtr, native_response);
      if (err != 0) {
        return new HashSet<TopicPartition>();
      }

      Set<TopicPartition> result = new HashSet<TopicPartition>();
      NativeDataParser nativeDataParser = getNativeDataParser(native_response);
      while (nativeDataParser.HasData()) {
        result.add(nativeDataParser.getNextTopicPartition());
      }
      return result;
    }
    
    public Set<TopicPartition> assignment() {
      // TODO Should this be a byteBuffer instead?
      NativeData native_response = new NativeData();
      int err = AssignmentList(_clntPtr, native_response);
      if (err != 0) {
        return new HashSet<TopicPartition>();
      }

      Set<TopicPartition> result = new HashSet<TopicPartition>();
      NativeDataParser nativeDataParser = getNativeDataParser(native_response);
      while (nativeDataParser.HasData()) {
        result.add(nativeDataParser.getNextTopicPartition());
      }
      return result;
    }

    protected NativeDataParser getNativeDataParser(NativeData native_response) {
      return new NativeDataParser(native_response);
    }

    public Set<String> subscription() {
      MarlinStringArrayWrapperImpl result = new MarlinStringArrayWrapperImpl();
      int err = SubscriptionList(_clntPtr, result);

      if (err != 0) {
        return new HashSet<String>();
      }
      return result.GetStringSet();
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) throws KafkaException {
      String[] topicArr = new String[topics.size()];
      int i = 0;

      for (String topic : topics) {
        topicArr[i] = topic;
        ++i;
      }

      MarlinRebalanceCallbackWrapperImpl rebalancecb = null;
      if (callback != null)
        rebalancecb = new MarlinRebalanceCallbackWrapperImpl(callback);

      int err = SubscribeTopics(_clntPtr, topicArr, rebalancecb);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not subscribe to topics");
      }
    }

    public void assign(Collection<TopicPartition> partitions) {
      String[] topicArr = new String[partitions.size()];
      int[] feedIdArr = new int[partitions.size()];
      int i = 0;

      for (TopicPartition partition : partitions) {
        topicArr[i] = partition.topic();
        feedIdArr[i++] = partition.partition();
      }

      int err = AssignFeeds(_clntPtr, topicArr, feedIdArr);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not assign partitions");
      }
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
      MarlinRebalanceCallbackWrapperImpl rebalancecb = null;
      if (callback != null)
        rebalancecb = new MarlinRebalanceCallbackWrapperImpl(callback);

      String patternString = pattern.toString();

      if (!checkPatternValid(patternString)) {
        throw MarlinClient.jniErrToException(Errno.EINVAL, "Could not subscribe, as invalid pattern " +
                                                           patternString + " is passed");
      }

      int err = SubscribeRegex(_clntPtr, patternString, rebalancecb);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not subscribe to pattern " + patternString);
      }
    }

    public void unsubscribe() {
      int err = Unsubscribe(_clntPtr);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "unsubscribe failed");
      }
    }

    public Map<TopicPartition, List<ListenerRecord>> poll(long timeout) {
      // Workaround for poll(0) to make the workflow similar to Apache Kafka
      if (timeout == 0) {
        return new HashMap<>();
      }

      NativeData native_response = new NativeData();

      int err = Poll(_clntPtr, timeout, native_response);
      if (err != 0 || (native_response.error() != 0)) {
        throw MarlinClient.jniErrToException(err, "poll failed");
      }

      NativeDataParser nativeDataParser = getNativeDataParser(native_response);
      Map<TopicPartition, List<ListenerRecord>> listenerRecords = nativeDataParser.parseListenerRecords(recordStripStreamPath);
      maybeUpdateMetrics(listenerRecords);
      return listenerRecords;
    }

    public void commitSync() {
      MarlinCommitCallbackWrapperImpl cbwrapper = (interceptors == null) ? null :
			 new MarlinCommitCallbackWrapperImpl(null /*callback */, interceptors);
      int err = CommitAll(_clntPtr, true /* sync */, cbwrapper /*callback*/);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not commitSync()");
      }
    }

    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      MarlinCommitCallbackWrapperImpl cbwrapper = (interceptors == null) ? null :
			 new MarlinCommitCallbackWrapperImpl(null /*callback */, interceptors);
      commitMap(offsets, true /*sync*/, cbwrapper /*callback*/);
    }

    public void commitAsync() {
      MarlinCommitCallbackWrapperImpl cbwrapper = (interceptors == null) ? null :
			 new MarlinCommitCallbackWrapperImpl(null /*callback */, interceptors);
      int err = CommitAll(_clntPtr, false /* sync */, cbwrapper/*callback*/);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not commitSync()");
      }
    }

    public void commitAsync(OffsetCommitCallback callback) {
      MarlinCommitCallbackWrapperImpl cbwrapper = null;
      cbwrapper = new MarlinCommitCallbackWrapperImpl(callback, interceptors);
      int err = CommitAll(_clntPtr, false /* sync */, cbwrapper);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not commitAsync(callback)");
      }
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      MarlinCommitCallbackWrapperImpl cbwrapper = null;
      cbwrapper = new MarlinCommitCallbackWrapperImpl(callback, interceptors);

      commitMap(offsets, false /*sync*/, cbwrapper);
    }

    /*
     * @brief Refresher service that will perform a TopicRefreshListener::updatedTopics(String[] topics) when 
     *         there is a change in topics' metadata, such as topic deleted/created, num partitions changed.
     *         All the valid topics matching the regex will be included in the callback.
     *  @param [in] clntPtr
     *  @param [in] topicRegex. NULL regexTopic will stop the refresher thread.
     *  @param [in] TopicRefreshListener
     *  @return err. 0 if success
     */
    public void topicRefresherRegex(Pattern pattern, TopicRefreshRegexListener callback) {
      MarlinTopicRefreshRegexListenerWrapperImpl refreshcb = null;
      if (callback != null) {
        refreshcb = new MarlinTopicRefreshRegexListenerWrapperImpl(callback);
      }

      String patternString = null;

      if (pattern != null) {
        patternString = pattern.toString();

        if (!checkPatternValid(patternString)) {
          throw MarlinClient.jniErrToException(Errno.EINVAL, "Could not refresh topics, as invalid pattern " +
                                                             patternString + " is passed");
        }
      }

      int err = TopicRefresherRegex(_clntPtr, patternString, refreshcb);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not refresh topics for pattern " + patternString);
      }
    }

    /*
     * @brief Refresher service that will perform a TopicRefreshListener:::updatedTopics(String[] topics) when
     *         there is a change in topics' metadata, such as topic deleted/created, num partitions changed.
     *  @param [in] clntPtr
     *  @param [in]  list of topics. Empty topic list will stop the refresher thread.
     *  @param [in] TopicRefresherCallback
     *  @return err. 0 if success
     */
    public void topicRefresherList(Collection<String> topics, TopicRefreshListListener callback) {
      String[] topicArr = new String[topics.size()];
      int i = 0;

      for (String topic : topics) {
        topicArr[i] = topic;
        ++i;
      }

      MarlinTopicRefreshListListenerWrapperImpl refreshcb = null;
      if (callback != null) {
        refreshcb = new MarlinTopicRefreshListListenerWrapperImpl(callback);
      }

      int err = TopicRefresherList(_clntPtr, topicArr, refreshcb);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not refresh the list of topics");
      }
    }

    private void commitMap(final Map<TopicPartition, OffsetAndMetadata> offsets,
                           boolean commitType /* true == sync, false == async*/,
                           MarlinCommitCallbackWrapperImpl callback) {
      String[] topicArr = new String[offsets.size()];
      String[] metadataArr = new String[offsets.size()];
      int[] feedIdArr = new int[offsets.size()];
      long[] offsetArr = new long[offsets.size()];
      int i = 0;


      Iterator it = offsets.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry)it.next();
        TopicPartition partition = (TopicPartition) entry.getKey();
        topicArr[i] = partition.topic();
        feedIdArr[i] = partition.partition();
        offsetArr[i] = ((OffsetAndMetadata) entry.getValue()).offset();
        metadataArr[i++] = ((OffsetAndMetadata) entry.getValue()).metadata();
      }

      int err = Commit(_clntPtr, topicArr, feedIdArr, offsetArr, metadataArr, commitType, callback);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not commit");
      }
    }

    private void seekInternal(long offset, Collection<TopicPartition> partitions) throws KafkaException {
      String[] topicArr = new String[partitions.size()];
      int[] feedIdArr = new int[partitions.size()];
      long[] offsetArr = new long[partitions.size()];
      int i = 0;

      for (TopicPartition partition : partitions) {
        topicArr[i] = partition.topic();
        feedIdArr[i] = partition.partition();
        offsetArr[i++] = offset;
      }

      int err = Seek(_clntPtr, topicArr, feedIdArr, offsetArr);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not seek");
      }
    }

    public void seek(TopicPartition partition, long offset) {
      seekInternal(offset, Arrays.asList(partition));
    }

    public void seekToBeginning(Collection<TopicPartition> partitions) {
      seekInternal(0, partitions);
    }

    public void seekToEnd(Collection<TopicPartition> partitions) {
      seekInternal(Long.MAX_VALUE, partitions);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
      return endOffsets(partitions, this.rpcTimeoutMs);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, int timeoutMS) {
      String[] topicArr = new String[partitions.size()];
      int[] feedIdArr = new int[partitions.size()];
      long[] outOffsets = new long[partitions.size()];
      int i = 0;

      for (TopicPartition partition : partitions) {
        topicArr[i] = partition.topic();
        feedIdArr[i++] = partition.partition();
      }

      int err = EndOffsets(_clntPtr, topicArr, feedIdArr, outOffsets, timeoutMS);

      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "endOffsets failed");
      }

      Map<TopicPartition, Long> outMap = new HashMap<TopicPartition, Long>();
      for (i = 0; i < outOffsets.length; i++) {
        outMap.put(new TopicPartition(topicArr[i], feedIdArr[i]), outOffsets[i]);
      }
      return outMap;
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
      return beginningOffsets(partitions, this.rpcTimeoutMs); 
    }
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, int timeoutMS) {

      Map<TopicPartition, Long> tsSearch = new HashMap<>();
      for (TopicPartition tp : partitions) {
        tsSearch.put(tp, EARLIEST_TIMESTAMP);
      }
      Map<TopicPartition, OffsetAndTimestamp> out = offsetsForTimes(tsSearch, timeoutMS);

      Map<TopicPartition, Long> outMap = new HashMap<TopicPartition, Long>();

      for (TopicPartition p : partitions) {
        OffsetAndTimestamp val = out.get(p);
        long offset = (val == null) ? 0 : val.offset();
        outMap.put(new TopicPartition(p.topic(), p.partition()), offset);
      }

      return outMap;
    }

    public long position(TopicPartition partition) throws
      KafkaException {
      NativeData native_response = new NativeData();
      int err = QueryPosition(_clntPtr, partition.topic(), partition.partition(),
                              native_response);
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not query position");
      }
      return native_response.long_data[0];
    }

    public OffsetAndMetadata committed(TopicPartition partition) throws
      KafkaException {
      MarlinOffsetAndMetadataWrapperImpl native_response = new MarlinOffsetAndMetadataWrapperImpl();
      int err = QueryCursor(_clntPtr, partition.topic(),
                            partition.partition(), native_response);
      if (err == Errno.ENOMSG) {
        //Cursor for topic-partition does not exist
        return null;
      }
      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Could not query committed offset");
      }

      return native_response.getOffsetAndMetadata();
    }

    public List<PartitionInfo> getTopicInfo(String topic) throws KafkaException {
      int nfeeds = GetTopicInfo(_clntPtr, topic);

      if (nfeeds < 0) {
        throw new UnknownTopicOrPartitionException("could not get TopicInfo, err " + -nfeeds);
      }

      MarlinTopicInfo result = new MarlinTopicInfo(topic, nfeeds);
      return result.getKafkaPartitionInfo();
    }

    public Map<String, List<PartitionInfo>> listTopics() {
      return listTopics((String) null);
    }

    public Map<String, List<PartitionInfo>> listTopics(Pattern pattern) {
      String patternString = pattern.toString();

      if (!checkPatternValid(patternString)) {
        throw MarlinClient.jniErrToException(Errno.EINVAL, "Could not listTopics, as invalid pattern " +
                                                           patternString + " is passed");
      }

      return listTopics(pattern.toString());
    }

    public Map<String, List<PartitionInfo>> listTopics(String stream) {
      Map<String, List<PartitionInfo>> topicsToReturn = new HashMap<String, List<PartitionInfo>>();
      MarlinStringArrayWrapperImpl result = new MarlinStringArrayWrapperImpl();
      int err = GetTopicsFromStream(_clntPtr, stream, result);

      if (err == Errno.ESTALE) {
        /* bug24483: Retry one more time. */
        err = GetTopicsFromStream(_clntPtr, stream, result);
      }

      if (err != 0) {
        LOG.debug("Could not get list of topics for stream " + stream + ", err " + err);
        return topicsToReturn;
      }

      List<String> topics = result.GetStringList();
      int[] numParts = result.GetNumPartitions();

      if (topics.size() != numParts.length) {
        LOG.error("Could not get list of topics for stream " + stream +
                  ", got " + topics.size() + " topics, but got " + numParts.length
                  + " partitions");
        return topicsToReturn;
      }

      int i = 0;

      for (String topic : topics) {
        MarlinTopicInfo mti = new MarlinTopicInfo(topic, numParts[i]);
        topicsToReturn.put(topic, mti.getKafkaPartitionInfo());
        ++i;
      }

      return topicsToReturn;
    }


    public void pause(Collection<TopicPartition> partitions) {
      String[] topicArr = new String[partitions.size()];
      int[] feedIdArr = new int[partitions.size()];
      int i = 0;

      for (TopicPartition partition : partitions) {
        topicArr[i] = partition.topic();
        feedIdArr[i++] = partition.partition();
      }

      int err = Pause(_clntPtr, topicArr, feedIdArr);

      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Error while pausing topic partitions " + partitions);
      }

      return;
    }

    public void resume(Collection<TopicPartition> partitions) {
      String[] topicArr = new String[partitions.size()];
      int[] feedIdArr = new int[partitions.size()];
      int i = 0;

      for (TopicPartition partition : partitions) {
        topicArr[i] = partition.topic();
        feedIdArr[i++] = partition.partition();
      }

      int err = Resume(_clntPtr, topicArr, feedIdArr);

      if (err != 0) {
        throw MarlinClient.jniErrToException(err, "Error while resuming topic partitions " + partitions);
      }

      return;
    }

    public JoinGroupResponse join(JoinGroupDesc joinDesc, MarlinJoinCallback cb) {
      byte[] serialDesc = joinDesc.toByteArray();

      MarlinJoinCallbackWrapper cbWrapper = null;
      if (cb != null) {
        cbWrapper = new MarlinJoinCallbackWrapperImpl(cb);
      }

      byte[] resp = Join(_clntPtr, serialDesc, cbWrapper);
      JoinGroupResponse joinResp = null;
      try {
        joinResp = JoinGroupResponse.parseFrom(resp);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        e.printStackTrace();
        throw new KafkaException("Error parsing Join response");
      }
      return joinResp;
    }

    public void close(long timeout, TimeUnit timeUnit) {
      synchronized(this) {
        if (_clntPtr != 0)
          CloseListener(_clntPtr, timeUnit.toMillis(timeout));
        closed = true;
        _clntPtr = 0;
      }
    }

    public void wakeup() {
      Wakeup(_clntPtr);
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
      return offsetsForTimes(timestampsToSearch, this.rpcTimeoutMs);
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, int timeoutMS) {
      String[] topicsArr = new String[timestampsToSearch.size()];
      int[] feedArr = new int[timestampsToSearch.size()];
      long[] timeArr = new long[timestampsToSearch.size()];
      long[] outOffsets = new long[timestampsToSearch.size()];
      long[] outTimes = new long[timestampsToSearch.size()];
     
      int i = 0;
      for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
        topicsArr[i] = entry.getKey().topic();
        feedArr[i] = entry.getKey().partition();
        timeArr[i++] = entry.getValue();
      }
      int err = OffsetsForTimes(_clntPtr, topicsArr, feedArr, timeoutMS,
                                timeArr, outOffsets, outTimes);
      if (err == Errno.ENOSYS) {
        throw new KafkaException(Errno.toString(err) +
                                 " (" + err + ") " +
                                 " Please complete the upgrade of the servers");
      } else if (err != 0) {
        throw MarlinClient.jniErrToException(err, "OffsetsForTimes failed");
      }

      Map<TopicPartition, OffsetAndTimestamp> outMap = new HashMap<TopicPartition, OffsetAndTimestamp>();
      for (i = 0; i < outTimes.length; i++) {
        if (outTimes[i] != marlinInternalDefaults.getNoTimestamp()) {
          outMap.put(new TopicPartition(topicsArr[i], feedArr[i]),
                     new OffsetAndTimestamp(outOffsets[i], outTimes[i]));
        }
      }
      return outMap;
    }

    private boolean checkPatternValid(String pattern) {
      // Remove the stream-path from topic-name and verify topic regex
      String topic = pattern;
      int slashIdx = pattern.lastIndexOf('/');
      if (slashIdx >= 0) {
        String lastPart = pattern.substring(slashIdx + 1);
        int idx = lastPart.indexOf(':');
        if (idx >= 0)
          topic = lastPart.substring(idx + 1);
      }
      try {
        Pattern checkPattern = Pattern.compile(topic);
      } catch (Exception e) {
        return false;
      }
      return true;
    }

    public class MarlinOffsetAndMetadataWrapperImpl implements MarlinOffsetAndMetadataWrapper {
      private String metadata;
      private long offset;

      public MarlinOffsetAndMetadataWrapperImpl () {}

      @Override
      public void SetOffsetAndMetadata(String m, long o) {
        metadata = m;
        offset = o;
      }

      OffsetAndMetadata getOffsetAndMetadata() {
        return new OffsetAndMetadata(offset, metadata);
      }

    }

  public class MarlinStringArrayWrapperImpl implements MarlinStringArrayWrapper {
    private String topicNames;  // One giant string with all the topic names concatenated
    private int[] topicNameSizes;  // sizes of the topic names in the above string
    private int[] numPartitions;  // number of partitions per topic
    private int numTopics;  // number of topics in total (just for sanity)

    public MarlinStringArrayWrapperImpl() {}

    @Override
    public void SetStringArrayElements(String tn, int[] tns, int[] np, int nt) {
      topicNames = tn;
      topicNameSizes = tns;
      numPartitions = np;
      numTopics = nt;
    }

    public Set<String> GetStringSet() {
      Set<String> toReturn = new HashSet<String>();

      int offset = 0;
      for (int i = 0; i < numTopics; ++i) {
        String tname = topicNames.substring(offset, offset+topicNameSizes[i]);
        offset += topicNameSizes[i];

        toReturn.add(tname);
      }

      return toReturn;
    }

    public List<String> GetStringList() {
      List<String> toReturn = new ArrayList<String>(numTopics);

      int offset = 0;
      for (int i = 0; i < numTopics; ++i) {
        String tname = topicNames.substring(offset, offset+topicNameSizes[i]);
        offset += topicNameSizes[i];

        toReturn.add(tname);
      }

      return toReturn;
    }

    public int[] GetNumPartitions() {
      return numPartitions;
    }
  }

  public class MarlinRebalanceCallbackWrapperImpl implements MarlinRebalanceCallbackWrapper {
    private ConsumerRebalanceListener rebalanceListener;

    public MarlinRebalanceCallbackWrapperImpl(ConsumerRebalanceListener rl) {
      rebalanceListener = rl;
    }

    @Override
    public void onPartitionsAssigned(NativeData data) {
      if (rebalanceListener != null) {
        List<TopicPartition> feeds = new ArrayList<TopicPartition>();
        NativeDataParser dataParser = new NativeDataParser(data);
        while(dataParser.HasData()) {
          feeds.add(dataParser.getNextTopicPartition());
        }
        rebalanceListener.onPartitionsAssigned(feeds);
      }
    }

    @Override
    public void onPartitionsRevoked(NativeData data) {
      if (rebalanceListener != null) {
        List<TopicPartition> feeds = new ArrayList<TopicPartition>();
        NativeDataParser dataParser = new NativeDataParser(data);
        while(dataParser.HasData()) {
          feeds.add(dataParser.getNextTopicPartition());
        }
        rebalanceListener.onPartitionsRevoked(feeds);
      }
    }
  }

  public class MarlinCommitCallbackWrapperImpl implements MarlinCommitCallbackWrapper {
    private OffsetCommitCallback commitcb;
    private ConsumerInterceptors<?, ?> interceptors;

    public MarlinCommitCallbackWrapperImpl(OffsetCommitCallback cb,
                                           ConsumerInterceptors<?, ?> ints) {
      commitcb = cb;
      interceptors = ints;
    }

    @Override
    public void onComplete(NativeData data, long[] offsets, String[] metadatas, int errorCode) {

      if (interceptors == null && commitcb == null) {
        return;
      }

      Map<TopicPartition, OffsetAndMetadata> feedAndOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

      int index = 0;
      NativeDataParser dataParser = getNativeDataParser(data);
      while(dataParser.HasData()) {
        feedAndOffsets.put(dataParser.getNextTopicPartition(), new OffsetAndMetadata(offsets[index], metadatas[index]));
        index++;
      }

      if (interceptors != null) {
        interceptors.onCommit(feedAndOffsets);
      }

      if (commitcb != null) {
        commitcb.onComplete(feedAndOffsets, MarlinClient.jniErrToException(errorCode, null));
      }
    }
  }

  public class MarlinJoinCallbackWrapperImpl implements MarlinJoinCallbackWrapper {
    private MarlinJoinCallback joincb;

    public MarlinJoinCallbackWrapperImpl(MarlinJoinCallback cb) {
      joincb = cb;
    }

    @Override
    public void onJoin(byte[] data) {
      try {
        JoinGroupInfo joinInfo = JoinGroupInfo.parseFrom(data);
        joincb.onJoin(joinInfo);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          e.printStackTrace();
          return;
      }
    }

    @Override
    public void onRejoin(byte[] data) {
      try {
        JoinGroupInfo joinInfo = JoinGroupInfo.parseFrom(data);
        joincb.onRejoin(joinInfo);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          e.printStackTrace();
          return;
      }
    }
  }

  public class MarlinTopicRefreshRegexListenerWrapperImpl implements MarlinTopicRefreshRegexListenerWrapper {
    private TopicRefreshRegexListener regexListener;

    public MarlinTopicRefreshRegexListenerWrapperImpl(TopicRefreshRegexListener rl) {
      regexListener = rl;
    }

    @Override
    public void updatedTopics(NativeData data) {
      if (regexListener != null) {
        List<TopicPartition> feeds = new ArrayList<TopicPartition>();
        NativeDataParser dataParser = new NativeDataParser(data);
        Set<String> topicNames = new HashSet<String>();
        while(dataParser.HasData()) {
          TopicPartition tp = dataParser.getNextTopicPartition();
          topicNames.add(tp.topic());
        }
        regexListener.updatedTopics(topicNames);
      }
    }
  }

  public class MarlinTopicRefreshListListenerWrapperImpl implements MarlinTopicRefreshListListenerWrapper {
    private TopicRefreshListListener listListener;

    public MarlinTopicRefreshListListenerWrapperImpl(TopicRefreshListListener ll) {
      listListener = ll;
    }

    @Override
    public void updatedTopics(NativeData data) {
      if (listListener != null) {
        NativeDataParser dataParser = new NativeDataParser(data);
        Set<TopicPartition> topicFeeds = new HashSet<TopicPartition>();
        while(dataParser.HasData()) {
          topicFeeds.add(dataParser.getNextTopicPartition());
        }
        listListener.updatedTopics(topicFeeds);
      }
    }
  }

  private void maybeUpdateMetrics(Map<TopicPartition, List<ListenerRecord>> partitionRecords) {
      if (metricsEnabled) {
        long now = System.currentTimeMillis();
        List<ListenerRecord> records = partitionRecords.values().stream()
                .flatMap(Collection::stream).collect(Collectors.toList());
        int recordsCount = records.size();
        int fetchSize = records.stream()
                .mapToInt(rec -> rec.topic().length() +
                        (rec.key() == null ? 0 : rec.key().length) +
                        (rec.value() == null ? 0 : rec.value().length) +
                        Arrays.stream(rec.headers().toArray()).mapToInt(h -> h.key().length() + h.value().length).sum())
                .sum();
        sensors.bytesFetched.record(fetchSize, now);
        sensors.recordsFetched.record(recordsCount, now);
      }
  }

}
