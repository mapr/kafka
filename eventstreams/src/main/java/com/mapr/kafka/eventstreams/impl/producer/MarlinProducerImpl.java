/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.StreamsPartitioner;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.hadoop.fs.Path;
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
import com.mapr.fs.jni.MapRUserInfo;
import com.mapr.fs.jni.MarlinJniProducer;
import com.mapr.fs.jni.MarlinProducerResult;
import com.mapr.fs.MapRFileSystem;
import com.mapr.kafka.eventstreams.impl.MarlinClient;
import com.mapr.kafka.eventstreams.impl.MarlinTopicInfo;
import com.mapr.fs.proto.Marlinserver.MarlinConfigDefaults;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;
import com.mapr.fs.ShimLoader;
/*
 * This class actually makes the jni calls for the producer and handles the
 * callbacks coming from jni.
 */
public class MarlinProducerImpl extends MarlinJniProducer {

  static {
    ShimLoader.load();
  }

  protected static final Logger LOG = LoggerFactory.getLogger(MarlinProducerImpl.class);

  private static final int MAX_WORK_QUEUE_SIZE = 4;
  protected static final int SEND_BATCH_SIZE = 100;
  private static final AtomicLong curProducerId = new AtomicLong(1);
  private int MaxWorkQueues;
  private List<WorkQueue> workQueues;
  private List<Thread> workers;
  private Boolean multiFlushers;
  private Boolean shuttingdown;
  private final AtomicLong counter = new AtomicLong(0);
  private final AtomicLong outstandingMsgs = new AtomicLong(0);
  private Map<String, Integer> topicNameToNumPartitions;
  private StreamsPartitioner partitioner;
  protected long producerId;
  private String defaultClusterPath;
  private String defaultStreamName;
  private  boolean metricsEnabled;
  private Metrics metrics;
  private Sender.SenderMetrics sensors;
  protected final MarlinInternalDefaults marlinInternalDefaults =
                              MarlinInternalDefaults.getDefaultInstance();

  private static void checkProducerConfig(ProducerConfig config) throws KafkaException {
    MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

    // Just reading the configuration should check that the client.id
    // exists.  If it does not exist, then ProducerConfig throws not
    // found exception.
    try {
      String clientID = config.getString(mConfDef.getClientID());
    } catch (ConfigException e) {
      LOG.error("Invalid client id configuration");
      throw e;
    }

    try {
      Boolean mflushers = config.getBoolean(mConfDef.getParallelFlushersPerPartition());
    } catch (ConfigException e) {
      LOG.error("Invalid streams paralle flushers configuration");
      throw e;
    }

    try {
      long bufferMemory = config.getLong(mConfDef.getBufferMemory());
      if (bufferMemory <= 0) {
        throw new ConfigException("Buffer memory (" + bufferMemory + ") must be a positive number");
      }
    } catch (ConfigException e) {
      LOG.error("Invalid buffer memory configuration");
      throw e;
    }

    try {
      long bufferTime = config.getLong(mConfDef.getBufferTime());
      if (bufferTime < 0) {
        throw new ConfigException("buffer ms (" + bufferTime + ") cannot be negative number");
      }
    } catch (ConfigException e){
      LOG.error("Invalid streams buffer time configuration");
      throw e;
    }

    try {
      long metadataAge = config.getLong(mConfDef.getMetadataMaxAge());
      if (metadataAge < 0) {
        throw new ConfigException("metadata max age (" + metadataAge + ") cannot be negative number");
      }
    } catch (ConfigException e) {
      LOG.error("Invalid metadata max age configuration");
      throw e;
    }

  }

  public MarlinProducerImpl(ProducerConfig config) throws KafkaException {
    checkProducerConfig(config);

    MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

    this.multiFlushers = config.getBoolean(mConfDef.getParallelFlushersPerPartition());
    this.partitioner = config.getConfiguredInstance(mConfDef.getPartitioner(),
                                                    StreamsPartitioner.class);
    this.metricsEnabled = config.getBoolean(ProducerConfig.METRICS_ENABLED_CONFIG);
    if (metricsEnabled) {
      this.metrics = buildMetrics(config);
      this.sensors = buildSensors(metrics);
    }
    this.defaultStreamName = null;
    int rpcTimeoutMs = 0;
    boolean hardMount = true;
    boolean isIdempotent = false;
    try {
      this.defaultStreamName = config.getString(mConfDef.getProducerDefaultStream());
      rpcTimeoutMs = config.getInt(mConfDef.getRpcTimeout());
      hardMount = config.getBoolean(mConfDef.getHardMount());
      isIdempotent = config.getBoolean(mConfDef.getEnableIdempotenceConfig());
    } catch (ConfigException e) {
      // It is okay for the default stream name or timeout to be not set.
      // So, just ignore the exception here.
    }

    shuttingdown = false;
    MaxWorkQueues = MAX_WORK_QUEUE_SIZE;

    MapRUserInfo userInfo;
    try {
      userInfo = MapRFileSystem.CurrentUserInfo();
    } catch (IOException e) {
      throw new KafkaException("Could not create MarlinProducer", e);
    }

    _clntPtr = OpenProducer(config.getString(mConfDef.getClientID()),
                            rpcTimeoutMs,
                            hardMount,
                            config.getBoolean(mConfDef.getParallelFlushersPerPartition()),
                            isIdempotent,
                            config.getLong(mConfDef.getBufferMemory()),
                            config.getLong(mConfDef.getBufferTime()),
                            config.getLong(mConfDef.getMetadataMaxAge()),
                            defaultStreamName,
                            userInfo);

    producerId = curProducerId.getAndIncrement();
    if (_clntPtr == 0) {
      /**
       * <MAPR_ERROR>
       *   Unable to create MapRClient object for the cluster
       *   specified in hostname:port
       * </MAPR_ERROR>
       */
      throw new NetworkException("Could not create Producer. Please ensure"
                                 + " that the CLDB service is configured"
                                 + " properly and is available");
    }

    defaultClusterPath = GetDefaultClusterPath(_clntPtr);

    workQueues = new ArrayList<WorkQueue>();
    workers = new ArrayList<Thread>();
    for (int i = 0; i < MaxWorkQueues; ++i) {
      WorkQueue wq = new WorkQueue(1024 /*capacity*/);

      workQueues.add(wq);
      Thread worker = new Thread(new WorkerThread(wq, i));
      worker.setDaemon(true);
      workers.add(worker);
      worker.start();
    }

    topicNameToNumPartitions = new ConcurrentHashMap();
  }

  protected Metrics buildMetrics(ProducerConfig config){
    String clientId = config.getString(MarlinConfigDefaults.getDefaultInstance().getClientID());
    Time time = Time.SYSTEM;
    Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
    MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            .tags(metricTags);
    List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
            MetricsReporter.class,
            Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
    JmxReporter jmxReporter = new JmxReporter();
    jmxReporter.configure(config.originals());
    reporters.add(jmxReporter);
    MetricsContext metricsContext = new KafkaMetricsContext("kafka.producer",
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
    return new Metrics(metricConfig, reporters, time, metricsContext);
  }

  protected Sender.SenderMetrics buildSensors(Metrics metrics) {
    Sender.SenderMetrics sensors = new Sender.SenderMetrics(new SenderMetricsRegistry(metrics),
            null, null, Time.SYSTEM);

    //remove unsupported metrics from sensors we use
    metrics.removeMetric(sensors.metrics.recordsPerRequestAvg);

    return sensors;
  }

  public Map<MetricName, ? extends Metric>  metrics(){
    return metricsEnabled ? metrics.metrics() : Collections.EMPTY_MAP;
  }

  protected RecordMetadata getDummyRecordMetadata(String topic) {
    return new RecordMetadata(
            new TopicPartition(topic, -1),
            -1L,
            0,
            ConsumerRecord.NO_TIMESTAMP,
            0,
            0);
  }

  public <K, V> Future<RecordMetadata> send(ProducerRecord<K, V> record, int feed,
                                            byte[] serializedKey, byte[] serializedValue,
                                            Callback callback) throws KafkaException {
    return do_send(record.topic(), feed, record.key(), serializedKey,
                record.value(), serializedValue, callback,
                marlinInternalDefaults.getNoTimestamp());
  }

  protected Future<RecordMetadata> do_send(String topic, int feed, Object keyObj, byte[] key,
      Object valueObj, byte[] value,
      Callback callback, long timestamp) throws KafkaException {

    if (shuttingdown) {
      LOG.error("Cannot send message when producer is shutting down.");
      if (callback != null)
        callback.onCompletion(getDummyRecordMetadata(topic),
            new RuntimeException("Producer is shutting down. Cannot send."));
      return new FutureFailure(new RuntimeException("Producer is shutting down. Cannot send."));
    }

    // Only use partitioner if feed is not specified, otherwise, all feed related
    // verification (e.g. if feedid < number of feeds in topic, > 0, etc.) happen in
    // our C MarlinProducer
    if (feed == -1) {
      int numParts = 0;

      // We should strip the /mapr/<default cluster name> from the topic name
      if (topic.startsWith(defaultClusterPath)) {
        topic = topic.substring(defaultClusterPath.length());
      }

      try {
        numParts = getNumPartitions(topic);
      } catch (KafkaException e) {
        LOG.error("Cannot send message, got an error while fetching number of partitions "
            + e);
        if (callback != null)
          callback.onCompletion(getDummyRecordMetadata(topic),
              new IllegalArgumentException(e.toString()));
        return new FutureFailure(new IllegalArgumentException(e.toString()));
      }

      feed = this.partitioner.partition(topic, keyObj, key, valueObj, value, numParts);
    }

    MarlinProducerResultImpl result = getMarlinProducerResultImpl(topic, feed, callback,
                                                                  key == null ? -1 : key.length,
                                                                  value == null ? -1 : value.length);

    ProducerRecordJob job = getProducerRecordJob(result, key, value, timestamp);

    long queueNumber  = 0;
    // if multiple flushers are allowed and we are using round-robin (no partition id,
    // no key), then just spread the messages across all java work queues.  But if
    // we are using either 1) single flusher, or 2) hashing or partition id specified,
    // then try to keep the ones going to the same topic-partition in the same
    // java work queue.
    if (multiFlushers && feed == -1 && key == null) {
      queueNumber += (counter.getAndIncrement() % Long.MAX_VALUE);
    } else {
      queueNumber += ((topic.length() + ((feed > 0) ? feed : 0)) % Long.MAX_VALUE);
    }

    int qid = ((int) (queueNumber % MaxWorkQueues));
    try {
      workQueues.get(qid).enqueue(job);  // blocks if the queue is full
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return new FutureFailure(e);
    }

    outstandingMsgs.getAndIncrement();

    return new MarlinFuture(result);
  }

  protected ProducerRecordJob getProducerRecordJob(MarlinProducerResultImpl result, byte[] key,  byte[] value, long timestamp) {
    return new ProducerRecordJob(result, key, value, timestamp);
  }

  protected ProducerRecordJob getProducerRecordJob() {
    return new ProducerRecordJob();
  }

  protected MarlinProducerResultImpl getMarlinProducerResultImpl(String topic, int feed, Callback callback,
                                                                  int serKeySz, int serValSz) {
    return new MarlinProducerResultImpl(topic, feed, callback, serKeySz, serValSz);
  }

  public void flush() throws KafkaException {
    for (int i = 0; i < MaxWorkQueues; ++i) {
      workQueues.get(i).enqueueFlushAndWait();
    }
    int err = Flush(_clntPtr);
    if (err != 0) {
      throw MarlinClient.jniErrToException(err, "could not flush to MarlinProducer");
    }
  }

  public void handleJniCallbacks(int length, long[] offsets, long[] timestamps,
                                 MarlinProducerResult[] results, int feedid, int errorCode) {

    LOG.debug("Handling callback for " + length);
    try {
      for (int i = 0; i < length; i++) {
        MarlinProducerResult result = results[i];
        if (result != null) {
          Exception ex = null;
          if (errorCode < 0)
            errorCode = -errorCode;
          if (errorCode == Errno.EACCES) {
            MarlinProducerResultImpl resultImpl = (MarlinProducerResultImpl) result;
            ex = new TopicAuthorizationException(resultImpl.getTopic());
          } else {
            ex = MarlinClient.jniErrToException(errorCode, null);
          }
          if (ex != null)
            maybeRecordError(((MarlinProducerResultImpl) result).getTopic());
          result.done(feedid, offsets[i], timestamps[i], ex);
          result.onCompletion();
        } else {
          LOG.error("producer.handleJniCallbacks() got null as result");
        }
      }
    } catch (Throwable e) {
      LOG.error("Hitting exception during handleJniCallbacks");
      LOG.error("Exception " + e);
    } finally {
      long prevValue = outstandingMsgs.getAndAdd(-1*length);
      if (prevValue < length) {
        LOG.error("outstandingMsgs are " + prevValue + " but we got results for "
            + length);
      }
      if (outstandingMsgs.get() == 0) {
        synchronized(outstandingMsgs) {
          if (outstandingMsgs.get() == 0) {
            try {
              outstandingMsgs.notifyAll();
            } catch (Exception e) {
              LOG.error("producer.handleJniCallbacks() exception while closing " + e);
            }
          }
        }
      }
    }
  }

  public void handleJniTopicMetadata(String topicNames, int[] topicNameSizes, int[] numPartitionsForTopic, int arraySize) {
    LOG.debug("Handling topic metadata changes for " + arraySize + " topics");

    try {
      int offset = 0;
      for (int i = 0; i < arraySize; i++) {
        String tname = topicNames.substring(offset, offset+topicNameSizes[i]);
        offset += topicNameSizes[i];

        if (numPartitionsForTopic[i] == 0) {
          // remove topic
          LOG.debug("Removing topic metadata for " + tname);
          topicNameToNumPartitions.remove(tname);
        } else {
          LOG.debug("Adding topic metadata for " + tname + " with " +
                    numPartitionsForTopic[i] + " partitions");
          topicNameToNumPartitions.put(tname, numPartitionsForTopic[i]);
        }
      }
    } catch (Throwable e) {
      LOG.error("Hitting exception during handleJniTopicMetadata");
      LOG.error("Exception " + e);
    }
  }

  private int getNumPartitions(String topic) throws KafkaException {
    Integer nfeeds = topicNameToNumPartitions.get(topic);

    if (nfeeds == null) {
      nfeeds = GetTopicInfo(_clntPtr, topic);
      if (nfeeds < 0) {
        maybeRecordError(topic);
        throw new UnknownTopicOrPartitionException("could not get TopicInfo, err " + -nfeeds);
      }
      topicNameToNumPartitions.put(topic, nfeeds);
    }
    return nfeeds;
  }


  public List<PartitionInfo> getTopicInfo(String topic) throws KafkaException {
    int nfeeds = getNumPartitions(topic);
    MarlinTopicInfo result = new MarlinTopicInfo(topic, nfeeds);
    return result.getKafkaPartitionInfo();
  }

  public void close() {
    LOG.debug("Producer is shutting down");
    synchronized(this) {
      if (shuttingdown) {
        LOG.debug("Producer shutdown already in progress");
        return;
      }

      shuttingdown = true;  // prevents from any send() from succeeding
    }
    closeInternal();
  }

  @Deprecated
  public void close(long timeout, TimeUnit unit) throws InterruptedException {
    close(Duration.ofMillis(unit.toMillis(timeout)));
  }

  public void close(Duration duration) throws InterruptedException {
    LOG.debug("Producer is shutting down");
    synchronized(this) {
      if (shuttingdown) {
        LOG.debug("Producer shutdown already in progress");
        return;
      }

      shuttingdown = true;  // prevents from any send() from succeeding
    }

    long timeoutMs = duration.toMillis();
    TimeUnit unit = TimeUnit.MILLISECONDS;
    Thread closeThread = new Thread(new CloseThread());
    closeThread.setDaemon(true);
    closeThread.start();
    if (timeoutMs <= 0) {
      return;
    } else {
      closeThread.join(unit.toMillis(timeoutMs));
    }
  }

  protected void closeInternal() {
    LOG.debug("Producer actually shutting down");

    flush();
    // mark all workqueues to be closed -- wakes up all the workers and they will start draining the queue.
    for (int i = 0; i < MaxWorkQueues; ++i) {
      workQueues.get(i).shutdown();
    }

    if (outstandingMsgs.get() != 0) {
      synchronized(outstandingMsgs) {
        if (outstandingMsgs.get() != 0) {
          try {
            outstandingMsgs.wait();
          } catch (Exception e) {
            LOG.error("producer.close() exception while waiting on outstanding messages " + e);
          }
        }
      }
    }

    for (int i = 0; i < MaxWorkQueues; ++i) {
      try {
        workers.get(i).join();
      } catch (InterruptedException e) {
        throw new InterruptException(e);
      }
    }
    LOG.debug("Calling jni producer close");
    CloseProducer(_clntPtr);
    LOG.debug("Returned from jni producer close");
    MaxWorkQueues = 0;
    _clntPtr = 0;
  }

  private static class FutureFailure implements Future<RecordMetadata> {

      private final ExecutionException exception;

      public FutureFailure(Exception exception) {
          this.exception = new ExecutionException(exception);
      }

      @Override
      public boolean cancel(boolean interrupt) {
          return false;
      }

      @Override
      public RecordMetadata get() throws ExecutionException {
          throw this.exception;
      }

      @Override
      public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
          throw this.exception;
      }

      @Override
      public boolean isCancelled() {
          return false;
      }

      @Override
      public boolean isDone() {
          return true;
      }
  }



  private class WorkQueue {
    private ProducerRecordJob[] precords;
    private int capacity;
    private int tailIdx;
    private int headIdx;
    private boolean shuttingdown;
    private boolean notifyOnDequeue;
    private boolean notifyOnEnqueue;

    public WorkQueue(int maxSz) {
      capacity = maxSz;
      precords = new ProducerRecordJob[capacity];
      tailIdx = headIdx = 0;
      shuttingdown = false;
      notifyOnDequeue = false;
      notifyOnEnqueue = false;
    }

    public synchronized void shutdown() {
      shuttingdown = true;
      this.notifyAll();
    }

    private boolean isEmpty() {
      return tailIdx == headIdx;
    }

    // Consider queue to be full at size 'capacity - 1'.
    private boolean isFull() {
      return (headIdx == tailIdx - 1 ||
              (headIdx == capacity - 1 && tailIdx == 0));
    }

    private int size() {
      if (isEmpty())
        return 0;
      else if (tailIdx < headIdx)
        return headIdx - tailIdx;
      else
        return capacity - tailIdx + headIdx;
    }

    public void enqueueFlushAndWait() {
      ProducerRecordJob flushJob = getProducerRecordJob();
      synchronized (flushJob) {
        enqueue(flushJob);
        try {
          while (!flushJob.getFlushJobDone())
            flushJob.wait();
        } catch (Exception e) {
          LOG.error("enqueueFlushAndWait flushJob.wait() encountered " + e);
        }
      }
    }

    public synchronized void enqueue(ProducerRecordJob rec) {
      if (shuttingdown) {
        LOG.error("enqueue job failed:  producer shutting down");
        throw new RuntimeException("enqueue job failed:  producer shutting down");
      }

      while (isFull()) {
        try {
          notifyOnDequeue = true;
          this.wait();
        } catch (Exception e) {
          LOG.error("enqueue encountered " + e);
        }
      }
      notifyOnDequeue = false;

      precords[headIdx] = rec;
      headIdx = (headIdx + 1) % capacity;
      assert(headIdx != tailIdx);
      if (notifyOnEnqueue)
        this.notify();
    }

    private ProducerRecordJob dequeueInternal() {
      assert(!isEmpty());

      ProducerRecordJob rec = precords[tailIdx];
      precords[tailIdx] = null;
      tailIdx = (tailIdx + 1) % capacity;
      return rec;
    }

    public synchronized int dequeue(ProducerRecordJob [] retList) {
      while (isEmpty() && !shuttingdown) {
        try {
          notifyOnEnqueue = true;
          this.wait();
        } catch (Exception e) {}
      }
      notifyOnEnqueue = false;

      // wait for 1ms to get a bigger batch if possible.
      if ((size() < SEND_BATCH_SIZE) && !shuttingdown) {
        try {
          this.wait(1);
        } catch (Exception e) {}
      }

      int retSz = size();
      assert(shuttingdown || retSz > 0);
      if (retSz > SEND_BATCH_SIZE)
        retSz = SEND_BATCH_SIZE;

      if (retSz > 0) {
        int idx = 0;
        while (idx < retSz) {
          retList[idx] = dequeueInternal();
          ++idx;
        }
        assert(idx == retSz);
        this.notify();
      }

      return retSz;
    }
  }

  private class CloseThread implements Runnable {

    public void run() {
      assert (shuttingdown == true);
      closeInternal();
    }
  }

  protected class WorkerThread implements Runnable {

    protected class WorkerState {
      protected int maxBytesToSend = 500*SEND_BATCH_SIZE; // assume that each message will be ~500 bytes
      protected List<ProducerRecordJob> flushJobs = new ArrayList<ProducerRecordJob>();
      protected int[] feedIDs = new int[SEND_BATCH_SIZE];
      protected ProducerRecordJob[] recList = new ProducerRecordJob[SEND_BATCH_SIZE];
      protected byte[] toSend = new byte[maxBytesToSend];
      protected long[] timestamps = new long[SEND_BATCH_SIZE];
      protected int[] numHeaders = new int[SEND_BATCH_SIZE];
      protected int[] byteOffsets;
      protected MarlinProducerResultImpl[] results;
      protected int byteSize = 0;
    }

    private WorkQueue wq;
    private int id;
    protected WorkerState ws = new WorkerState();

    public WorkerThread(WorkQueue wq, int id) {
      this.id = id;
      this.wq = wq;
    }

    public void run() {
      try {
        while (true) {
          int recSz = wq.dequeue(ws.recList);

          // drain everything from the queue before exiting.  dequeue() only returns
          // null if it we are shutting down and there aren't any jobs left in the workQueue.
          if (recSz == 0) {
            LOG.debug("Worker thread " + id + " joining");
            return;
          }

          LOG.debug("Worker thread " + id + " got " + recSz + " jobs");

          int numMsgJobs = encodeJniData(ws, recSz);

          assert(recSz == numMsgJobs + ws.flushJobs.size());

          if (numMsgJobs > 0 ) {
            try {
              int err = Send(_clntPtr, producerId, ws.toSend, ws.byteSize, ws.byteOffsets,
                  ws.feedIDs, ws.timestamps, ws.numHeaders, ws.results, numMsgJobs);
              if (err != 0) {
                throw MarlinClient.jniErrToException(err, "could not send to MarlinProducer");
              }
            } catch (ApiException e) {
              LOG.error("Exception occurred during message send:", e);

              // Now loop through all the pending sends and call callback if we have one.
              for (int i = 0; i < recSz; ++i) {
                if (ws.recList[i].isFlushJob()) continue;

                MarlinProducerResultImpl result = ws.recList[i].getResult();
                result.done(result.getFeed(), -1, marlinInternalDefaults.getNoTimestamp(), e);
                result.onCompletion();
              }
            }
          }
          maybeUpdateMetrics(ws);

          for (int i = 0; i < ws.flushJobs.size(); ++i) {
            ProducerRecordJob notifyJob = ws.flushJobs.get(i);
            synchronized(notifyJob) {
              notifyJob.markFlushJobDone();
              notifyJob.notify();
            }
          }
          ws.flushJobs.clear();
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  /*
   * @brief Encode the jni send buffers based on kafka-0.9.0 records.
   * @return Number of messages to be sent.
   */
  protected int encodeJniData(WorkerThread.WorkerState ws, int recSz) {
    // Get number of offsets neeeded to represent record info.
    int numOffsets = recSz * 3; // Topic name, key, val
    ws.byteOffsets = new int[numOffsets];
    ws.results = new MarlinProducerResultImpl[recSz];
    ws.byteSize = 0;

    int byteOffsetIndex = 0;
    int numMsgJobs = 0;
    for (int i = 0; i < recSz; ++i) {
      ProducerRecordJob rec = ws.recList[i];
      if (rec.isFlushJob()) {
        ws.flushJobs.add(rec);
        continue;
      }
      ws.feedIDs[numMsgJobs] = rec.getResult().getFeed();
      ws.results[numMsgJobs] = rec.getResult();
      ws.byteSize += rec.getTopic().length;
      ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      if (rec.getKey() != null)
        ws.byteSize += rec.getKey().length;
      ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      if (rec.getValue() != null)
        ws.byteSize += rec.getValue().length;
      ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      ws.timestamps[numMsgJobs] = rec.getTimestamp();
      assert (ws.numHeaders[numMsgJobs] == 0);
      numMsgJobs++;
    }

    if (ws.maxBytesToSend < ws.byteSize){
      LOG.debug("Worker thread increasing byte array from " + ws.maxBytesToSend + " to " + ws.byteSize);
      ws.toSend = new byte[ws.byteSize];
      ws.maxBytesToSend = ws.byteSize;
    }

    ByteBuffer toSendBuffer = ByteBuffer.wrap(ws.toSend);
    numMsgJobs = 0;
    for (int i = 0; i < recSz; ++i) {
      ProducerRecordJob rec = ws.recList[i];
      if (rec.isFlushJob()) {
        continue;
      }

      toSendBuffer.put(rec.getTopic());
      if (rec.getKey() != null) {
        toSendBuffer.put(rec.getKey());
      }
      if (rec.getValue() != null) {
        toSendBuffer.put(rec.getValue());
      }
      numMsgJobs++;
    }

    return numMsgJobs;

  }

  private void maybeUpdateMetrics(WorkerThread.WorkerState ws) {
    if (metricsEnabled){
      long now = System.currentTimeMillis();
      List<ProducerRecordJob> records = Arrays.stream(ws.recList)
              .filter(rec -> rec != null && !rec.isFlushJob()).collect(Collectors.toList());

      sensors.recordsPerRequestSensor.record(records.size(), now);

      for (ProducerRecordJob record: records) {
        sensors.maxRecordSizeSensor.record(record.estimatedSizeInBytes(), now);

        String topic = new String(record.getTopic());
        sensors.maybeRegisterTopicMetrics(topic);

        String topicRecordsCountName = "topic." + topic + ".records-per-batch";
        Sensor topicRecordCount = Objects.requireNonNull(sensors.metrics.getSensor(topicRecordsCountName));
        topicRecordCount.record(1, now);

        String topicByteRateName = "topic." + topic + ".bytes";
        Sensor topicByteRate = Objects.requireNonNull(sensors.metrics.getSensor(topicByteRateName));
        topicByteRate.record(record.estimatedSizeInBytes());
      }
    }
  }

  private void maybeRecordError(String topic){
    if (metricsEnabled) {
      sensors.maybeRegisterTopicMetrics(topic);
      long now = System.currentTimeMillis();
      sensors.errorSensor.record(1, now);
      Sensor topicErrorSensor = this.sensors.metrics.getSensor("topic." + topic + ".record-errors");
      if (topicErrorSensor != null)
        topicErrorSensor.record(1, now);
    }
  }
}
