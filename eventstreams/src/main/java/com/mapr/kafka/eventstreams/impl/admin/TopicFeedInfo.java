/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.admin;

import com.mapr.fs.proto.Dbserver.TimeRange;
import com.mapr.fs.proto.Marlinserver.LcPartitionStatus;
import com.mapr.fs.proto.Marlinserver.TopicFeedStatInfo;

import java.util.ArrayList;
import java.util.List;

public class TopicFeedInfo {
  private static final long MARLIN_INVALID_SEQNUM = -1;
  TopicFeedStatInfo stat;
  List<CursorInfo> cursorList;
  LcPartitionStatus logCompactionStatus;

  public TopicFeedInfo(TopicFeedStatInfo sinfo, List<CursorInfo> cursorList,
                       LcPartitionStatus logCompactionStatus) {
    this.stat = sinfo;
    this.cursorList = cursorList;
    this.logCompactionStatus = logCompactionStatus;
  }

  public TopicFeedInfo(int feedId) {
    this.stat = TopicFeedStatInfo.newBuilder()
                                 .setFeedId(feedId)
                                 .build();
    this.cursorList = new ArrayList<CursorInfo>();
  }

  public TopicFeedInfo(int feedId, LcPartitionStatus logCompactionStatus) {
    this.stat = TopicFeedStatInfo.newBuilder()
                                 .setFeedId(feedId)
                                 .build();
    this.logCompactionStatus = logCompactionStatus;
    this.cursorList = new ArrayList<CursorInfo>();
  }

  // stat
  public TopicFeedStatInfo stat() { return stat; }

  public LcPartitionStatus logCompactionStatus() { return logCompactionStatus; }

  public void updateStat(TopicFeedStatInfo statNew) {
    TopicFeedStatInfo merged = mergeStats(this.stat, statNew);
    this.stat = merged;
  }

  private TopicFeedStatInfo mergeStats(TopicFeedStatInfo s1,
                                       TopicFeedStatInfo s2) {
    TopicFeedStatInfo.Builder minfo = TopicFeedStatInfo.newBuilder();
    minfo.setFeedId(s1.getFeedId());

    long psize = s1.getPhysicalSize() + s2.getPhysicalSize();
    minfo.setPhysicalSize(psize);

    long lsize = s1.getLogicalSize() + s2.getLogicalSize();
    minfo.setLogicalSize(lsize);

    long minSeq = Math.min(s1.getMinSeq(), s2.getMinSeq());
    minfo.setMinSeq(minSeq);

    if (s2.getMaxSeq() == MARLIN_INVALID_SEQNUM) {
      /* BUG 26111: s2.getMaxSeq() will be MARLIN_INVALID_SEQNUM
       * only when server responds with MARLIN_INVALID_SEQNUM 
       * (after mfs.feature.db.streams.v6.support feature is enabled)
       */
      minfo.setMaxSeq(MARLIN_INVALID_SEQNUM);
    }else {
      long maxSeq = Math.max(s1.getMaxSeq(), s2.getMaxSeq());
      minfo.setMaxSeq(maxSeq);
    }
    long tmin = 0;
    long tmax = 0;
    if (s1.hasTimeRange() && s2.hasTimeRange()) {
      tmin = Math.min(s1.getTimeRange().getMinTS(),
                      s2.getTimeRange().getMinTS());
      tmax = Math.max(s1.getTimeRange().getMaxTS(),
                      s2.getTimeRange().getMaxTS());
    } else if (s1.hasTimeRange()) {
      tmin = s1.getTimeRange().getMinTS();
      tmax = s1.getTimeRange().getMaxTS();
    } else if (s2.hasTimeRange()) {
      tmin = s2.getTimeRange().getMinTS();
      tmax = s2.getTimeRange().getMaxTS();
    }
    if (tmax != 0) {
      TimeRange trange = TimeRange.newBuilder()
                                  .setMinTS(tmin)
                                  .setMaxTS(tmax)
                                  .build();
      minfo.setTimeRange(trange);
    }

    TopicFeedStatInfo copyFrom = s1;
    if ((s2.getMaxSeq() > s1.getMaxSeq()) || (s2.getMaxSeq() == MARLIN_INVALID_SEQNUM)) {
      // The server list should point to the head of the topic partition.
      copyFrom = s2;
    }
    if (copyFrom.hasFid())
      minfo.setFid(copyFrom.getFid());
    if (copyFrom.hasMaster())
      minfo.setMaster(copyFrom.getMaster());
    if (copyFrom.hasMasterPort())
      minfo.setMasterPort(copyFrom.getMasterPort());
    minfo.addAllServers(copyFrom.getServersList());
    minfo.addAllServerPorts(copyFrom.getServerPortsList());
    return minfo.build();
  }

  // Cursors
  public void addCursor(CursorInfo ci) { cursorList.add(ci); }
  public List<CursorInfo> cursorList() { return cursorList; }
}
