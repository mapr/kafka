/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams;

import com.mapr.fs.ShimLoader;
import com.mapr.kafka.eventstreams.impl.MessageStore;
import com.mapr.kafka.eventstreams.impl.admin.MStreamDescriptor;
import com.mapr.kafka.eventstreams.impl.admin.MTopicDescriptor;
import com.mapr.kafka.eventstreams.impl.admin.MarlinAdminImpl;
import org.apache.hadoop.conf.Configuration;
import org.ojai.DocumentConstants;
import org.ojai.store.DocumentStore;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * This class provides the entry point to accessing MapR streams for analytics
 * purposes.
 * <br/><br/>
 * The user can invoke the getMessageStore() methods to get
 * <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a>
 * object representing the underlying message
 * store.
 * <br/><br/>
 * The static String's are field names of different fields in each message.
 * They can be used to project specific fields while running find() on the
 * returned 
 * <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a>
 * object.
 */
public class Streams {
  static {
    com.mapr.fs.ShimLoader.load();
  }

  /**
   * Field name representing the unique id of the message
   */
  public static final String ID              = DocumentConstants.ID_KEY;

  /**
   * Field name representing partition id of the message
   */
  public static final String PARTITION       = "partition";

  /**
   * Field name representing topic of the message
   */
  public static final String TOPIC           = "topic";

  /**
   * Field name representing offset of the message
   */
  public static final String OFFSET          = "offset";

  /**
   * Field name representing producer of the message
   */
  public static final String PRODUCER        = "producer";

  /**
   * Field name representing the message key
   */
  public static final String KEY             = "key";

  /**
   * Field name representing the message value(user provided message)
   */
  public static final String VALUE           = "value";

  /* Bug 19945 : We don't support querying timestamps yet
   * Field name representing timestamp of the message
   */
  //public static final String TIMESTAMP       = "timestamp";

  /**
   * Configuration parameter to set the maximum number of background threads to scan the
   * documents(default int: 16)
   */
  public static final String MAX_SCANNER_THREADS = "streams.analytics.max_scanner_threads";

  /**
   * Configuration parameter that controls the maximum cache memory to use while iterating
   * through the scanned documents(default long: 100MB)
   */
  public static final String MAX_CACHE_MEMORY = "streams.analytics.cache_memory";

  /**
   * Returns a read-only DocumentStore object representing the stream of the given path
   *
   * @param streamPath the path to stream
   * @return <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a> object
   * @throws IOException
   */
  public static DocumentStore getMessageStore(String streamPath) throws IOException {
    return new MessageStore(streamPath, new Configuration());
  }

  /**
   * Returns a read-only DocumentStore object representing the stream of the given path
   *
   * @param streamPath the path to stream
   * @param conf hadoop configuration object
   *             (can be used to set specific values for max scanner threads and max cache memory)
   * @return <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a> object
   * @throws IOException
   */
  public static DocumentStore getMessageStore(String streamPath, Configuration conf)
    throws IOException {
     return new MessageStore(streamPath, conf, (String[])null);  
  }

  /**
   * Returns a read-only DocumentStore object representing the stream of the given path
   * User can also provide only a subset of topics to read messages from
   *
   * @param streamPath the path to stream
   * @param topics list of topics to read messages from
   * @return <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a> object
   * @throws IOException
   */
  public static DocumentStore getMessageStore(String streamPath, String... topics) throws IOException {
    return new MessageStore(streamPath, new Configuration(), topics);
  }

  /**
   * Returns a read-only DocumentStore object representing the stream of the given path
   * User can also provide only a subset of topics to read messages from
   *
   * @param streamPath the path to stream
   * @param conf hadoop configuration object
   *             (can be used to set specific values for max scanner threads and max cache memory)
   * @param topics list of topics to read messages from
   * @return <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a> object
   * @throws IOException
   */
  public static DocumentStore getMessageStore(String streamPath, Configuration conf, String... topics) throws IOException {
    return new MessageStore(streamPath, conf, topics);
  }

  /**
   * Returns a read-only DocumentStore object representing the stream of the given path
   * User can also provide a regex representing all topics to read messages from
   *
   * @param streamPath the path to stream
   * @param regex <a href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Pattern</a> representing topics to read messages from
   * @return <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a> object
   * @throws IOException
   */
  public static DocumentStore getMessageStore(String streamPath, Pattern regex) throws IOException {
    return new MessageStore(streamPath, new Configuration(), regex);
  }

  /**
   * Returns a read-only DocumentStore object representing the stream of the given path
   * User can also provide a regex representing all topics to read messages from
   *
   * @param streamPath the path to stream
   * @param conf hadoop configuration object
   *             (can be used to set specific values for max scanner threads and max cache memory)
   * @param regex <a href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Pattern</a> representing topics to read messages from
   * @return <a href="http://ojai.github.io/javadocs/latest/org/ojai/store/DocumentStore.html">DocumentStore</a> object
   * @throws IOException
   */
  public static DocumentStore getMessageStore(String streamPath, Configuration conf, Pattern regex) throws IOException {
    return new MessageStore(streamPath, conf, regex);
  }

  /**
   * Creates and returns an Admin instance with specified configuration.
   * @return an instance of Admin interface
   */
  public static Admin newAdmin(Configuration c)
      throws IOException {
    return new MarlinAdminImpl(c);
  }

  /**
   * Creates and returns an Admin instance with specified configuration.
   * @return an instance of Admin interface
   */
  public static Admin newAdmin(Configuration c, String cluster)
      throws IOException {
    return new MarlinAdminImpl(c, cluster);
  }

  /**
   * Creates and returns a StreamDescriptor instance.
   * @return an instance of StreamDescriptor interface
   */
  public static StreamDescriptor newStreamDescriptor() {
    return new MStreamDescriptor();
  }

  /**
   * Creates and returns a TopicDescriptor instance.
   * @return an instance of TopicDescriptor interface
   */
  public static TopicDescriptor newTopicDescriptor() {
    return new MTopicDescriptor();
  }
}
