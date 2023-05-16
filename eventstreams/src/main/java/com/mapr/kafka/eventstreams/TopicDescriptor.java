package com.mapr.kafka.eventstreams;

/**
 * A TopicDescriptor contains the details about a topic such as the
 * number of partitions, the timestamptype, log compaction settingetc.
 */
public interface TopicDescriptor extends Cloneable {

  /**
   * @return the number of partitions for the topic.
   */
  public int getPartitions();

  /**
   * Sets the number of partitions for the topic.
   * @param numPartitions the number of partitions
   */
  public void setPartitions(int numPartitions);

  /**
   * Sets timestamp type
   * @param timestampType can be createtime or logappendtime
   */
  public void setTimestampType(TimestampType timestampType);

  /**
   * @return timestampType
   */
  public  TimestampType getTimestampType();
}
