/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.admin;

import java.util.List;

public class AssignInfo {
  String streamName;
  String topic;
  String listenerID;
  long assignSeqNum;
  String [] listeners;
  int numListeners;
  List<List<Integer>> listenerAssignment;

  public void Init(String streamName, String topic, String listenerID,
                   long assignSeqNum, String[] listeners, int
                   numListeners, List<List<Integer>> listenerAssignment) {
    this.streamName = streamName;
    this.topic = topic;
    this.listenerID = listenerID;
    this.assignSeqNum = assignSeqNum;
    this.listeners = listeners;
    this.numListeners = numListeners;
    this.listenerAssignment = listenerAssignment;
  }

  public String streamName() { return streamName; }
  public String topic() { return topic; }
  public String listenerID() { return listenerID; }
  public long assignSeqNum() { return assignSeqNum; }
  public String[] listeners() { return listeners; }
  public int numListeners() { return numListeners; }
  public List<Integer> listenerAssignment(int i) { 
    return listenerAssignment.get(i); 
  }

}
