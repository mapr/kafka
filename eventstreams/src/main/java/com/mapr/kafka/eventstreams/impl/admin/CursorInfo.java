/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.admin;

public class CursorInfo {
  String streamName;
  String topic;
  String listenerID;
  int feedId;
  long cursor;
  long timestamp;
  TopicFeedInfo topicFeedInfo;

  public void Init(String streamName, String topic, String listenerID, int
                   feedId, long cursor, long timestamp) {
    this.streamName = streamName;
    this.topic = topic;
    this.listenerID = listenerID;
    this.feedId = feedId;
    this.cursor = cursor;
    this.timestamp = timestamp;
    this.topicFeedInfo = null;
  }

  public String streamName() { return streamName; }
  public String topic() { return topic; }
  public String listenerID() { return listenerID; }
  public int feedId() { return feedId; }
  public long cursor() { return cursor; }
  public long timestamp() { return timestamp; }
  public void setTopicFeedInfo(TopicFeedInfo topicFeedInfo) { this.topicFeedInfo = topicFeedInfo; }
  public TopicFeedInfo topicFeedInfo() { return topicFeedInfo; }
}
