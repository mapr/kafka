/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps;

import org.apache.hadoop.fs.Path;

import com.mapr.fs.jni.Errno;

@Deprecated
public class KWPSCommon {
  private KWPSCommon() {}

  public static final String KWPS_USER_MAPR = "mapr";

  public static final String KWPS_NEW_TOPIC_OWN_VOLUME = "own.volume";

  public static final String KWPS_ROOT_VOLUME = "mapr.kwps.root";

  public static final String KWPS_VOLNAME_PREFIX = "mapr.kwps.vol.";

  public static final String KWPS_ROOT_FOLDER = "/var/mapr/" + KWPS_ROOT_VOLUME;

  public static final Path KWPS_ROOT_FOLDER_PATH = new Path(KWPS_ROOT_FOLDER);

  public static final String KWPS_TOPICS_FOLDER = KWPS_ROOT_FOLDER + "/topics/";

  public static final Path KWPS_TOPICS_FOLDER_PATH = new Path(KWPS_TOPICS_FOLDER);

  static String errMsg(int errNo, String msg, Object... args) {
    return String.format(msg, args) + " Error: " + Errno.toString(errNo);
  }

  public static String getStreamPath(String topicName) {
    return new StringBuilder(KWPS_TOPICS_FOLDER).append(topicName).append("/stream").toString();
  }

  public static String getStreamParentPath(final String topicName) {
    return new StringBuilder(KWPSCommon.KWPS_TOPICS_FOLDER).append(topicName).toString();
  }

  public static String getVolumeName(final String topicName) {
    return new StringBuilder(KWPSCommon.KWPS_VOLNAME_PREFIX).append(topicName).toString();
  }

}
