/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl;

import com.mapr.fs.ShimLoader;

import com.mapr.baseutils.Errno;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.errors.DuplicateSequenceException;
import org.apache.kafka.common.errors.UnknownProducerIdException;

/* 
 * Base abstract class that implements all the functionality that is common to
 * both Listener and Producer.
 */
public class MarlinClient {

  static {
    ShimLoader.load();
  }

 /* Map all the jni exception to a real Java runtime exception. 
   * TODO - Go through all possible error codes that can be returned from the
   * jni layer and mapp them to the right exception message.
   */
  public static RuntimeException jniErrToException(int errCode, String msg) {
    if (errCode < 0)
      errCode = -errCode;
    switch (errCode) {
    case Errno.SUCCESS:
      return null;
    case Errno.EPERM:
      return new UnknownTopicOrPartitionException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.ENOENT:
      return new UnknownTopicOrPartitionException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.ESTALE:
      return new UnknownTopicOrPartitionException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.EACCES:
      return new IllegalArgumentException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.ETIME:
      return new TimeoutException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.EIO:
      return new CorruptRecordException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.EINVAL:
      return new IllegalArgumentException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.EEXIST:
      return new DuplicateSequenceException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.EKEYEXPIRED:
      return new UnknownProducerIdException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    case Errno.EINTR:
      return new WakeupException();
    default:
      return new KafkaException(Errno.toString(errCode) + " (" + errCode + ") " + msg);
    }
  }
}
