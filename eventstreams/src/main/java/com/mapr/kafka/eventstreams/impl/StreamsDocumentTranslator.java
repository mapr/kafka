/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl;

import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;


public class StreamsDocumentTranslator {

  static final Logger LOG = LoggerFactory.getLogger(StreamsDocumentTranslator.class);

  private List<Object> rowValueDocs;
  private String rowIdStr;
  private int indexInRow;
  private int msgsCount;
  private int rowPartition;
  private String rowTopic;
  private long rowOffset;
  private String producer;
  private List<Integer> projectedPathIds;

  private static final FieldPath ROW_PARTITION_PATH = FieldPath.parseFrom(MarlinRowKeyDecoder.PARTITION);
  private static final FieldPath ROW_TOPIC_PATH = FieldPath.parseFrom(MarlinRowKeyDecoder.TOPIC);
  private static final FieldPath ROW_OFFSET_PATH = FieldPath.parseFrom(MarlinRowKeyDecoder.OFFSET);

  private static final MarlinInternalDefaults MDEF = MarlinInternalDefaults.getDefaultInstance();
  private static final FieldPath ROW_PRODUCER_PATH   = FieldPath.parseFrom(MDEF.getFMsgsProd());
  private static final FieldPath ROW_MSGS_LIST_PATH  = FieldPath.parseFrom(MDEF.getFMsgsList());
  private static final FieldPath ROW_MSGS_COUNT_PATH = FieldPath.parseFrom(MDEF.getFMsgsCount());

  public StreamsDocumentTranslator(Document dbRecord,
                                   List<Integer> pathIds) throws Exception {
    rowIdStr = dbRecord.getIdString();
    Document rowKeyDoc = MarlinRowKeyDecoder.decodeMsgKey(rowIdStr);
    rowPartition = rowKeyDoc.getInt(ROW_PARTITION_PATH);
    rowTopic     = rowKeyDoc.getString(ROW_TOPIC_PATH);
    rowOffset    = rowKeyDoc.getLong(ROW_OFFSET_PATH);
    rowValueDocs = dbRecord.getList(ROW_MSGS_LIST_PATH);
    indexInRow = 0;
    msgsCount = (rowValueDocs != null)?rowValueDocs.size():dbRecord.getByte(ROW_MSGS_COUNT_PATH);
    producer = dbRecord.getString(ROW_PRODUCER_PATH);
    projectedPathIds = pathIds;
  }

  public boolean hasNext() {
    return indexInRow < msgsCount;
  }

  public Document next() {
    if (hasNext()) {
      int cur = indexInRow++;
      Document valueDoc = null;

      if (rowValueDocs != null &&
          rowValueDocs.size() > 0) {
        valueDoc = (Document)rowValueDocs.get(cur);
      }

      return new StreamsDocument(rowIdStr, rowPartition, rowTopic, rowOffset,
                                 producer, valueDoc, cur, projectedPathIds);
    }

    throw new NoSuchElementException("next() called after hasNext() " + 
                                     "returned false.");
  }
}
