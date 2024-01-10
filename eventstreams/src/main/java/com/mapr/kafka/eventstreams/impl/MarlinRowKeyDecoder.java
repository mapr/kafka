/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl;

import com.mapr.db.rowcol.DBDocumentImpl;
import com.mapr.fs.proto.Marlinserver.MarlinInternalDefaults;
import org.ojai.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarlinRowKeyDecoder {

  static final Logger LOG = LoggerFactory.getLogger(MarlinRowKeyDecoder.class);

  public static String TOPIC = "topic";
  public static String PARTITION = "partition";
  public static String TOPIC_UNIQ = "topicUniq";
  public static String OFFSET = "offset";
  public static String CONSUMER_GROUP = "consumerGroup";

  private static final MarlinInternalDefaults mdef = MarlinInternalDefaults.getDefaultInstance();

  public MarlinRowKeyDecoder() { }

  public static Document decodeMsgKey(String key)
      throws IllegalArgumentException {

    DBDocumentImpl doc = new DBDocumentImpl();
    int offset = 0;

    // Decode the row-key : p<partition>T<topicName>:<topicUniq><seq>
    offset = decodePartition(key, offset, doc);
    offset = decodeTopic(key, offset, doc);
    offset = decodeTopicUniq(key, offset, doc);
    offset = decodeSeq(key, offset, doc);
    return doc;
  }

  public static Document decodeTopicFeedKey(String key)
      throws IllegalArgumentException {

    DBDocumentImpl doc = new DBDocumentImpl();
    int offset = 0;

    // Decode the row-key : p<partition>T<topicName>:<topicUniq>
    offset = decodePartition(key, offset, doc);
    offset = decodeTopic(key, offset, doc);
    offset = decodeTopicUniq(key, offset, doc);
    return doc;
  }

  public static Document decodeCursorKey(String key)
      throws IllegalArgumentException {

    DBDocumentImpl doc = new DBDocumentImpl();
    int offset = 0;

    // Decode the row-key : cT<topicname>:<topicUniq>p<feedid>G<lGId>
    offset += matchCursorPrefix(key, offset);
    offset = decodeTopic(key, offset, doc);
    offset = decodeTopicUniq(key, offset, doc);
    offset = decodePartition(key, offset, doc);
    offset = decodeGroup(key, offset, doc);
    return doc;
  }

  public static Document decodeAssignKey(String key)
     throws IllegalArgumentException {

    DBDocumentImpl doc = new DBDocumentImpl();
    int offset = 0;

    // Decode the row-key : aT<topicname>:<topicUniq>G<lGId>
    offset = matchAssignPrefix(key, offset);
    offset = decodeTopic(key, offset, doc);
    offset = decodeTopicUniq(key, offset, doc);
    offset = decodeGroup(key, offset, doc);
    return doc;
  }

  private static int decodePartition(String key, int offset, Document doc)
      throws IllegalArgumentException {

    String prefix = mdef.getKeyPrefixFeedId();
    if (!matchPrefix(key, offset, prefix))
      throw new IllegalArgumentException("Invalid partition prefix at offset " +
                                         offset + " in key " + key);
    offset +=  prefix.length();

    long lval = decodeHexBytes(key, offset, mdef.getKeyWidthFeedId());
    if (lval < 0)
      throw new IllegalArgumentException("Invalid partition id at offset " +
                                         offset + " in key " + key);

    offset += mdef.getKeyWidthFeedId();
    doc.set(PARTITION, (int)lval);
    return offset;
  }

  private static int decodeTopic(String key, int offset, Document doc)
      throws IllegalArgumentException {

    String prefix = mdef.getKeyPrefixTopicName();
    if (!matchPrefix(key, offset, prefix))
      throw new IllegalArgumentException("Invalid topic prefix at offset " +
                                         offset + " in key " + key);
    offset +=  prefix.length();

    int index = key.indexOf(':', offset);
    if (index < -1)
      throw new IllegalArgumentException("Topic delimiter ':' missing" +
                                         " in key " + key);
    if (index - offset == 0)
      throw new IllegalArgumentException("Empty topic in key " + key);

    doc.set(TOPIC, key.substring(offset, index));
    offset = index + 1; // consume ':' also
    return offset;
  }

  private static int decodeTopicUniq(String key, int offset, Document doc)
      throws IllegalArgumentException {

    long lval = decodeHexBytes(key, offset, mdef.getKeyWidthTopicUniq());
    if (lval < 0)
      throw new IllegalArgumentException("Invalid topicUniq at offset " +
                                         offset + " in key " + key);
    doc.set(TOPIC_UNIQ, (int)lval);
    offset += mdef.getKeyWidthTopicUniq();
    return offset;
  }

  private static int decodeSeq(String key, int offset, Document doc)
      throws IllegalArgumentException {

    long lval = decodeHexBytes(key, offset, mdef.getKeyWidthSeq());
    if (lval < 0)
      throw new IllegalArgumentException("Invalid seq at offset " +
                                         offset + " in key " + key);
    doc.set(OFFSET, lval);
    return offset;
  }

  private static int decodeGroup(String key, int offset, Document doc)
      throws IllegalArgumentException {

    String prefix = mdef.getKeyPrefixGroup();
    if (!matchPrefix(key, offset, prefix))
      throw new IllegalArgumentException("Invalid group prefix at offset " +
                                         offset + " in key " + key);
    offset +=  prefix.length();

    if (offset == key.length())
      throw new IllegalArgumentException("Empty listener group id " +
                                         " in key " + key);

    doc.set(CONSUMER_GROUP, key.substring(offset));
    return key.length();
  }

  // decodes the string as hex bytes.
  // returns -1 on error, value on success
  private static long decodeHexBytes(String key, int offset, int width) {
    long value = 0;

    if (offset + width > key.length())
      return -1;

    for (int i = 0; i < width; ++ i) {
      long hval = isHexDigit(key.charAt(offset + i));
      if (hval < 0)
        return -1;

      value = value * 16 + hval;
    }
    return value;
  }

  private static long isHexDigit(char c) {
    if (((c >= '0') && (c <= '9')) || ((c >= 'a') && (c <= 'f')))
      return Character.digit(c, 16);
    else
      return -1;
  }

  private static boolean matchPrefix(String key, int offset, String prefix) {
    if (offset + prefix.length() > key.length())
      return false;

    String subStr = key.substring(offset, offset + prefix.length());
    return subStr.equals(prefix);
  }

  private static int matchCursorPrefix(String key, int offset)
      throws IllegalArgumentException {

    String prefix = mdef.getKeyCursorPrefix();
    if (!matchPrefix(key, offset, prefix))
      throw new IllegalArgumentException("Invalid cursor prefix at offset " +
                                         offset + " in key " + key);

    return offset + prefix.length();
  }

  private static int matchAssignPrefix(String key, int offset)
      throws IllegalArgumentException {

    String prefix = mdef.getKeyAssignPrefix();
    if (!matchPrefix(key, offset, prefix))
      throw new IllegalArgumentException("Invalid assign prefix at offset " +
                                         offset + " in key " + key);

    return offset + prefix.length();
  }
}
