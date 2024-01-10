/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl;

import static com.mapr.kafka.eventstreams.Streams.ID;
import static com.mapr.kafka.eventstreams.Streams.KEY;
import static com.mapr.kafka.eventstreams.Streams.OFFSET;
import static com.mapr.kafka.eventstreams.Streams.PARTITION;
import static com.mapr.kafka.eventstreams.Streams.PRODUCER;
import static com.mapr.kafka.eventstreams.Streams.TOPIC;
import static com.mapr.kafka.eventstreams.Streams.VALUE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ojai.Document;
import org.ojai.DocumentConstants;
import org.ojai.FieldPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.db.rowcol.DBDocumentImpl;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;

public class StreamsDocument extends DBDocumentImpl {

  static final Logger LOG = LoggerFactory.getLogger(StreamsDocument.class);

  private static final MarlinInternalDefaults MDEF = MarlinInternalDefaults.getDefaultInstance();

  private static Map<String, Integer> fieldIdMap;
  private static final int ID_FIELDID             = 1;
  private static final int PARTITION_FIELDID      = 2;
  private static final int TOPIC_FIELDID          = 3;
  private static final int OFFSET_FIELDID         = 4;
  private static final int PRODUCER_FIELDID       = 5;
  private static final int KEY_FIELDID            = 6;
  private static final int VALUE_FIELDID          = 7;
//  private static final int TIMESTAMP_FIELDID      = 8;

  private static final FieldPath ID_PATH        = FieldPath.parseFrom(ID);
  private static final FieldPath PARTITION_PATH = FieldPath.parseFrom(PARTITION);
  private static final FieldPath TOPIC_PATH     = FieldPath.parseFrom(TOPIC);
  private static final FieldPath OFFSET_PATH    = FieldPath.parseFrom(OFFSET);
  private static final FieldPath PRODUCER_PATH  = FieldPath.parseFrom(PRODUCER);
  private static final FieldPath KEY_PATH       = FieldPath.parseFrom(KEY);
  private static final FieldPath VALUE_PATH     = FieldPath.parseFrom(VALUE);

  private static final FieldPath DB_KEY_PATH    = FieldPath.parseFrom(MDEF.getFMsgsKey());
  private static final FieldPath DB_VAL_PATH    = FieldPath.parseFrom(MDEF.getFMsgsValue());
  private static final FieldPath MSG_OFFSET_DELTA    = FieldPath.parseFrom(MDEF.getFMsgsOffsetDelta());

  private int documentSize;
  private int msgsOffsetDelta;

  static {
    fieldIdMap = new HashMap<String, Integer>();
    fieldIdMap.put(ID, ID_FIELDID);
    fieldIdMap.put(PARTITION, PARTITION_FIELDID);
    fieldIdMap.put(TOPIC, TOPIC_FIELDID);
    fieldIdMap.put(OFFSET, OFFSET_FIELDID);
    fieldIdMap.put(PRODUCER, PRODUCER_FIELDID);
    fieldIdMap.put(KEY, KEY_FIELDID);
    fieldIdMap.put(VALUE, VALUE_FIELDID);

    // Bug 19945: Disable get'ting timestamp since DBDocumentImpl does not
    // support it yet.
    //fieldIdMap.put(TIMESTAMP, TIMESTAMP_FIELDID);

    // StreamsDocument cannot have Topic Uniq and Consumer Group info
  }

  public static List<Integer> getProjectionIdList(List<FieldPath> ps) {

    if (ps == null) {
      return null;
    }

    List<Integer> projections = new ArrayList<Integer>();
    for (FieldPath path : ps) {

      if (path == null) {
        throw new IllegalArgumentException("NULL projection not allowed");
      }

      String str = path.asPathString();

      // Bug 19945: Disable projecting timestamp
      //if (str.equals(TIMESTAMP)) {
      //  throw new UnsupportedOperationException("Projecting timestamp is not supported yet");
      //}

      Integer i = fieldIdMap.get(str);
      if (i == null) {
        throw new IllegalArgumentException("Invalid field " + path);
      }

      projections.add(i);
    }

    return projections;
  }

  public StreamsDocument(String rowIdStr,
                         int rowPartition,
                         String rowTopic,
                         long rowOffset,
                         String producer,
                         // long ts,
                         Document rowMsgDoc,
                         int indexInRow,
                         List<Integer> projectedPathIds) {

    LOG.debug("Topic: {} Partition: {} Offset: {}", rowTopic, rowPartition, rowOffset);

    documentSize = 0;
    msgsOffsetDelta = 0;
    boolean isMsgDocPresent = false;
    if (rowMsgDoc != null)
      isMsgDocPresent = true;

    Integer delta = isMsgDocPresent?rowMsgDoc.getIntObj(MSG_OFFSET_DELTA):null;
    if (delta != null) {
      msgsOffsetDelta = delta.intValue();
    }

    if ((projectedPathIds != null) &&
        (projectedPathIds.size() > 0)) {
      for (Integer pathId : projectedPathIds) {
        switch (pathId) {
          case ID_FIELDID:
            String docIdStr = new StringBuilder(rowIdStr).append(":").append(indexInRow).toString();
            set(ID_PATH, docIdStr);
            break;

          case PARTITION_FIELDID:
            set(PARTITION_PATH, rowPartition);
            break;

          case TOPIC_FIELDID:
            set(TOPIC_PATH, rowTopic);
            LOG.debug("set topic");
            break;

          case OFFSET_FIELDID:
            set(OFFSET_PATH, rowOffset + indexInRow + msgsOffsetDelta);
            break;

          case PRODUCER_FIELDID:
            if (producer != null) {
              set(PRODUCER_PATH, producer);
            }
            break;

          case KEY_FIELDID:
            ByteBuffer rowKey = isMsgDocPresent?rowMsgDoc.getBinary(DB_KEY_PATH):null;
            if (rowKey != null) {
              documentSize += rowKey.limit();
              set(KEY_PATH, rowKey);
            }
            break;

          case VALUE_FIELDID:
            ByteBuffer rowValue = isMsgDocPresent?rowMsgDoc.getBinary(DB_VAL_PATH):null;
            set(VALUE_PATH, rowValue);
            documentSize += rowValue.limit();
            break;

          // Bug 19945: Disable get'ting timestamp since DBDocumentImpl does not
          // support it yet.
          //case TIMESTAMP_FIELDID:
          //  set(TIMESTAMP, ts);
          //  break;

          default:
            assert(false);
        }
      }
    } else {

      String docIdStr = new StringBuilder(rowIdStr).append(":").append(indexInRow).toString();
      set(ID_PATH, docIdStr);
      set(PARTITION_PATH, rowPartition);
      set(TOPIC_PATH, rowTopic);
      set(OFFSET_PATH, rowOffset + indexInRow + msgsOffsetDelta);

      if (producer != null)
        set(PRODUCER_PATH, producer);

      ByteBuffer rowKey = isMsgDocPresent?rowMsgDoc.getBinary(DB_KEY_PATH):null;
      if (rowKey != null) {
        documentSize = rowKey.limit();
        set(KEY_PATH, rowKey);
      }

      ByteBuffer rowValue = isMsgDocPresent?rowMsgDoc.getBinary(DB_VAL_PATH):null;
      if (rowValue != null) {
        set(VALUE_PATH, rowValue);
        documentSize += rowValue.limit();
      }
      // Bug 19945: Disable get'ting timestamp since DBDocumentImpl does not
      // support it yet.
      // set(TIMESTAMP, ts);
    }

    documentSize += (documentSize / 4); // constant multiple for other fields
  }

  public int size() {
    return documentSize;
  }
}
