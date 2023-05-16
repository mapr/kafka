/* Copyright (c) 2018 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams;

import java.util.Set;

public interface TopicRefreshRegexListener {

  void updatedTopics(Set<String> topics);

}
