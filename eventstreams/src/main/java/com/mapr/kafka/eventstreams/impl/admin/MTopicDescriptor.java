package com.mapr.kafka.eventstreams.impl.admin;

import com.mapr.kafka.eventstreams.TimestampType;
import com.mapr.kafka.eventstreams.TopicDescriptor;

public class MTopicDescriptor implements TopicDescriptor {

	int partitions;
	TimestampType timestampType;

	boolean hasPartitions;
	boolean hasTimestampType;

	public MTopicDescriptor() {
		hasPartitions = false;
		hasTimestampType = false;
	}

	@Override
	public int getPartitions() {
		return partitions;
	}

	@Override
	public void setPartitions(int numPartitions) {
		hasPartitions = true;
		partitions = numPartitions;
	}

	public boolean hasPartitions() {
		return hasPartitions;
	}

	@Override
	public void setTimestampType(TimestampType timestampType) {
		hasTimestampType = true;
		this.timestampType = timestampType;
	}

	@Override
	public TimestampType getTimestampType() {
		return timestampType;
	}

	public boolean hasTimestampType() {
		return hasTimestampType;
	}
}
