/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl;

import static com.mapr.kafka.eventstreams.Streams.MAX_SCANNER_THREADS;
import static com.mapr.kafka.eventstreams.Streams.MAX_CACHE_MEMORY;

import static com.mapr.kafka.eventstreams.Streams.KEY;
import static com.mapr.kafka.eventstreams.Streams.PRODUCER;
import static com.mapr.kafka.eventstreams.Streams.VALUE;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.ojai.Document;
import org.ojai.DocumentConstants;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;
import org.ojai.store.QueryResult;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;
import org.ojai.util.EmptyDocumentStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.db.TableSplitInternal;
import com.mapr.db.TabletInfo;
import com.mapr.db.impl.IdCodec;
import com.mapr.db.impl.MapRDBImpl;
import com.mapr.db.impl.TableDescriptorImpl;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;
import com.mapr.fs.proto.Marlinserver.MarlinTopicMetaEntry;
import com.mapr.streams.impl.MarlinSplitterCore;
import com.mapr.kafka.eventstreams.Admin;
import com.mapr.kafka.eventstreams.impl.admin.MarlinAdminImpl;
import com.mapr.kafka.eventstreams.impl.admin.TopicFeedInfo;
import com.mapr.kafka.eventstreams.Streams;

@SuppressWarnings("deprecation")
public class MessageStore implements DocumentStore {

  static final Logger LOG = LoggerFactory.getLogger(MessageStore.class);
  Table        table;
  List<QueryCondition> findConditions;
  int numParallelScans;
  long maxCacheMemory;

  static MarlinInternalDefaults mdef = MarlinInternalDefaults.getDefaultInstance();

  static String messageStartKey = String.format(mdef.getKeyPrefixFeedId() + mdef.getKeyFmtFeedId(), 0);
  static QueryCondition messageStartCondition =
    MapRDBImpl.newCondition()
    .is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, messageStartKey)
    .build();

  static final FieldPath PRODUCER_FIELD = FieldPath.parseFrom(mdef.getFMsgsProd());
  static final FieldPath MSG_COUNT_FIELD = FieldPath.parseFrom(mdef.getFMsgsCount());

  private class TopicMeta implements Comparable<TopicMeta> {
    String topic;
    List<Integer>  feedIds;

    public TopicMeta(String t, List<Integer> feeds) {
      topic = t;
      feedIds = feeds;
    }

    public String getTopic() {
      return topic;
    }

    public boolean hasFeedId(int id) {
      for (Integer feedId : feedIds) {
        if (feedId == id) {
          return true;
        }
      }

      return false;
    }

    public int compareTo(TopicMeta compareMeta) {
      int cmp = topic.compareTo(compareMeta.getTopic());
      return cmp;
    }
  }

  private void commonInit(String streamPath, Configuration conf, Pattern topicRegex, List<String> topics)
     throws IOException {

    Admin admin = Streams.newAdmin(new Configuration());

    table = MapRDBImpl.getTable(streamPath);

    TableDescriptor desc = table.getTableDescriptor();

    assert(desc instanceof TableDescriptorImpl);

    // check if the given path is a stream
    if (((TableDescriptorImpl)desc).isStream() == false) {
      throw new IOException("The path " + streamPath
          + " does not refer to a valid stream");
    }

    // Create a list of valid topics to scan
    Set<String> topicSet = new HashSet<String>();

    MarlinAdminImpl madmin = (MarlinAdminImpl)admin;
    Map<String, List<TopicFeedInfo>> topicMap = madmin.listTopicsForStream(streamPath);

    if (topics != null && topics.size() > 0) {
      Collections.sort(topics);
    }

    for (Map.Entry<String, List<TopicFeedInfo>> entry : topicMap.entrySet()) {

      // If the user provided list of topics, select valid topics from it
      if ((topics != null) && (topics.size() > 0)) {
        for (String topic : topics) {
          if (topic == null) {
            throw new IllegalArgumentException("NULL topic not allowed");
          }

          int cmp = topic.compareTo(entry.getKey());
          if (cmp > 0) {
            break;
          } else if (cmp == 0) {
            topicSet.add(entry.getKey());
          }
        }
      } else if (topicRegex != null) {
        // else, if user provided a topic regex, select topics that match regex
        Matcher m = topicRegex.matcher(entry.getKey());
        if (m.matches()) {
          topicSet.add(entry.getKey());
        }
      } else {
        // else, we need to scan all topics
        topicSet.add(entry.getKey());
      }
    }


    // Create a list of TopicMeta objects based on meta info and sort it
    // by topic name
    List<TopicMeta> topicMetaList = new ArrayList<TopicMeta>();

    for (String topic : topicSet) {
      MarlinTopicMetaEntry metaEntry = madmin.getTopicMetaEntry(streamPath + ":" + topic);
      if (metaEntry.getIsDeleted()) {
        continue;
      }

      LOG.debug("Topic: " + topic + ", feeds: " + metaEntry.getFeedIdsList());
      topicMetaList.add(new TopicMeta(topic, metaEntry.getFeedIdsList()));
    }

    admin.close();

    Collections.sort(topicMetaList);

    // get the marlin splits
    TabletInfo[] tablets = table.getTabletInfos(messageStartCondition);
    List<TableSplitInternal> splits = MarlinSplitterCore.getMarlinSplits(table.getName(), tablets);

    findConditions = new ArrayList<QueryCondition>();

    int feedPrefixLength = mdef.getKeyPrefixFeedId().length() + mdef.getKeyWidthFeedId();

    for (TableSplitInternal split : splits) {
      String splitStart = IdCodec.decodeString(split.getStartRow());
      String splitStop  = IdCodec.decodeString(split.getStopRow());
      String feedStr = splitStart.substring(mdef.getKeyPrefixFeedId().length(),
                                            feedPrefixLength);
      int feedId = Integer.parseInt(feedStr, 16);

      LOG.debug("Split: [" + splitStart + ", " + splitStop + ")");
      LOG.debug("Feed Str: " + feedStr + ", id: " + feedId);
      LOG.debug("[" + feedPrefixLength + "] " + splitStart.length() + " " + splitStop.length());

      String  splitStartTopic = null;
      String  splitStopTopic = null;

      String splitFeedPrefix = splitStart.substring(0, feedPrefixLength);

      if (splitStart.length() != feedPrefixLength) {
        splitStartTopic = splitStart.substring(feedPrefixLength + 1,
                                               splitStart.length() - 1); // handle the ';'
        LOG.debug("Split start topic = " + splitStartTopic);
      }

      if (splitStop.length() != feedPrefixLength &&
          splitStop.length() != 0) { // handle last split
        splitStopTopic = splitStop.substring(feedPrefixLength + 1,
                                             splitStop.length() - 1); // handle the ';'
        LOG.debug("Split stop topic = " + splitStopTopic);
      }

      for (TopicMeta topicMeta : topicMetaList) {
        // skip over all topics befor splitStartTopic(if splitStartTopicNull is false)
        if (splitStartTopic != null &&
            topicMeta.getTopic().compareTo(splitStartTopic) < 0) {
          continue;
        }

        // skip over all topics after splitStopTopic(if splitStopTopicNull is false)
        if (splitStopTopic != null &&
            topicMeta.getTopic().compareTo(splitStopTopic) >= 0) {
          break;
        }

        // skip over the topic if it does not have the feed
        if (topicMeta.hasFeedId(feedId) == false) {
          continue;
        }

        String topicStartKey = new String(splitFeedPrefix + mdef.getKeyPrefixTopicName()
                                          + topicMeta.getTopic() + ":");
        String topicStopKey  = new String(splitFeedPrefix + mdef.getKeyPrefixTopicName()
                                          + topicMeta.getTopic() + ";");

        QueryCondition findCondition = MapRDBImpl.newCondition()
                       .and()
                       .is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, topicStartKey)
                       .is(DocumentConstants.ID_FIELD, Op.LESS, topicStopKey)
                       .close().build();

        LOG.debug("Find condition: " + findCondition);
        findConditions.add(findCondition);
      }
    }

    // Default parallel scans while iterating through MarlinDocumentStream
    // Cap the number of threads to 16
    int maxParallelScans = conf.getInt(MAX_SCANNER_THREADS, 16);
    numParallelScans = findConditions.size() < maxParallelScans ? findConditions.size()
                                                                  : maxParallelScans;

    maxCacheMemory = conf.getLong(MAX_CACHE_MEMORY, 100 * 1024 * 1024);
  }

  public MessageStore(String streamPathInMapRFS, Configuration conf, String... topics) throws IOException {
    List<String> l = new ArrayList<String>();

    if (topics != null) {
      for (String topic : topics) {
        l.add(topic);
      }
    }

    commonInit(streamPathInMapRFS, conf, null/*topicRegex*/, l);
  }

  public MessageStore(String streamPathInMapRFS, Configuration conf, Pattern topicRegex) throws IOException {
    commonInit(streamPathInMapRFS, conf, topicRegex, null /*topics*/);
  }

  public int getNumSplits() {
    return findConditions.size();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public void flush() throws StoreException {
    throw new StoreException("flush() operation not supported on read-only DocumentStore");
  }

  @Override
  public DocumentStream find() throws StoreException {
    List<DocumentStream> dbDocumentStreams =
                                        new ArrayList<DocumentStream>();
    for (QueryCondition cond : findConditions) {
      dbDocumentStreams.add(table.find(cond));
    }

    MarlinDocumentStream mds = new MarlinDocumentStream(dbDocumentStreams, null, numParallelScans, maxCacheMemory);
    if (mds.exception != null) {
      throw mds.exception;
    }
    return mds;
  }

  @Override
  public DocumentStream find(String... paths)
  throws StoreException {
    List<FieldPath> ps = new ArrayList<FieldPath>();
    for (String path : paths) {
      ps.add(FieldPath.parseFrom(path));
    }

    return scan(ps);
  }

  @Override
  public DocumentStream find(FieldPath... paths) throws StoreException {
    List<FieldPath> ps = new ArrayList<FieldPath>();
    for (FieldPath path : paths) {
      ps.add(path);
    }

    return scan(ps);
  }

  private DocumentStream scan(List<FieldPath> paths)
      throws StoreException {
    List<FieldPath> jsonPaths = new ArrayList<FieldPath>();

    // Bug 21519: If we project specific fields without projecting the message list,
    // the logic in StreamsDocumentTranslator breaks since it depends on the message
    // list to generate MarlinRecord's. Disabling this logic for now to unblock QA.
    // If the projection list has producer, but not key or value
    // we add that to the JSON scan() projection list
    boolean projectProducer = false;
    boolean projectKey      = false;
    boolean projectValue    = false;

    for (FieldPath path : paths) {
      if (path == null) {
        throw new IllegalArgumentException("NULL projection path provided");
      }

      if (path.asPathString().equals(PRODUCER)) {
        projectProducer = true;
        continue;
      }

      if (path.asPathString().equals(KEY)) {
        projectKey = true;
        continue;
      }

      if (path.asPathString().equals(VALUE)) {
        projectValue = true;
        continue;
      }
    }

    if (projectProducer == true &&
        projectKey == false && projectValue == false) {

      // add to JSON scan() projection list
      jsonPaths.add(PRODUCER_FIELD);
      jsonPaths.add(MSG_COUNT_FIELD);
    }

    // If the user is not interested in producer, or key or value
    // we add only ID_FIELD in the projection list
    if (projectProducer == false && projectKey == false && projectValue == false) {
      jsonPaths.add(DocumentConstants.ID_FIELD);
      jsonPaths.add(MSG_COUNT_FIELD);
    }

    for (FieldPath path : jsonPaths) {
      LOG.debug("JSON DB projection: " + path);
    }

    List<DocumentStream> dbDocumentStreams =
                                        new ArrayList<DocumentStream>();
    for (QueryCondition cond : findConditions) {
      if (jsonPaths.size() > 0) {
        dbDocumentStreams.add(table.find(cond, jsonPaths.toArray(new FieldPath[jsonPaths.size()])));
      } else {
        dbDocumentStreams.add(table.find(cond));
      }
    }

    MarlinDocumentStream mds = new MarlinDocumentStream(dbDocumentStreams, paths, numParallelScans, maxCacheMemory);
    if (mds.exception != null) {
      throw mds.exception; 
    }

    return mds;
  }

  @Override
  public DocumentStream find(QueryCondition c)
  throws StoreException {
    throw new StoreException("scan(QueryCondition) operation not yet supported");
  }

  @Override
  public DocumentStream find(QueryCondition c,
      String... paths) throws StoreException {
    throw new StoreException("scan(QueryCondition, String...) operation not yet supported");
  }

  @Override
  public DocumentStream find(QueryCondition c,
      FieldPath... paths) throws StoreException {
    throw new StoreException("scan(QueryCondition, FieldPath...) operation not yet supported");
  }

  @Override
  public QueryResult find(Query query) throws StoreException {
    throw new StoreException("find(Query query) operation not yet supported");
  }

  @Override
  public DocumentStream findQuery(Query query) throws StoreException {
    throw new StoreException("findQuery(Query query) operation not yet supported");
  }

  @Override
  public DocumentStream findQuery(String queryJSON) throws StoreException {
    throw new StoreException("findQuery(String queryJSON) operation not yet supported");
  }

  @Override
  public void close() throws StoreException {
    table.close();
  }

  @Override
  public void insertOrReplace(Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(Value _id, Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(Document doc, FieldPath fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(Document doc, String fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(DocumentStream stream) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void update(Value _id, DocumentMutation m) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void delete(Value _id) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void delete(Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void delete(Document doc, FieldPath fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void delete(Document doc, String fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void delete(DocumentStream stream) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void delete(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void delete(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void insert(Value _id, Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insert(Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insert(Document doc, FieldPath fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insert(Document doc, String fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insert(DocumentStream stream) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void insert(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void insert(DocumentStream stream,
      String fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void replace(Value _id, Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void replace(Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void replace(Document doc, FieldPath fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void replace(Document doc, String fieldAsKey) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void replace(DocumentStream stream) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void replace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void replace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, byte inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, short inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, int inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, long inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, float inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, double inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(Value _id, String field, BigDecimal inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public boolean checkAndMutate(Value _id, QueryCondition condition,
      DocumentMutation m) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public boolean checkAndDelete(Value _id, QueryCondition condition)
  throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public boolean checkAndReplace(Value _id, QueryCondition condition,
      Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public String endTrackingWrites() throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void beginTrackingWrites() throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void beginTrackingWrites(String previousContext) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void clearTrackedWrites() throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insertOrReplace(String _id, Document r) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void update(String _id, DocumentMutation mutation) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void delete(String _id) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void insert(String _id, Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void replace(String _id, Document doc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, byte inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, short inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, int inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, long inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, float inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, double inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public void increment(String _id, String field, BigDecimal inc) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public boolean checkAndMutate(String _id, QueryCondition condition, DocumentMutation mutation)
      throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public boolean checkAndDelete(String _id, QueryCondition condition) throws StoreException {
    throw readOnlyStore();
  }

  @Override
  public boolean checkAndReplace(String _id, QueryCondition condition, Document doc)
      throws StoreException {
    throw readOnlyStore();
  }

  private static StoreException readOnlyStore() {
    return new StoreException("Operation not supported on read-only DocumentStore");
  }

  private static StoreException unsupportedOperation() {
    return new StoreException("Operation not supported on read-only DocumentStore");
  }

  @Override
  public Document findById(String _id) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(Value _id) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(String _id, String... fieldPaths) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(String _id, FieldPath... fieldPaths) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(Value _id, String... fieldPaths) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(Value _id, FieldPath... fieldPaths) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(String _id, QueryCondition condition) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(Value _id, QueryCondition condition) throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(String _id, QueryCondition c, String... fieldPaths)
      throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(String _id, QueryCondition condition, FieldPath... fieldPaths)
      throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(Value _id, QueryCondition condition, String... fieldPaths)
      throws StoreException {
    throw unsupportedOperation();
  }

  @Override
  public Document findById(Value _id, QueryCondition condition, FieldPath... fieldPaths)
      throws StoreException {
    throw unsupportedOperation();
  }

}
