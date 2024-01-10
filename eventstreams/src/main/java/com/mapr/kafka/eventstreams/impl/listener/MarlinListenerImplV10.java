package com.mapr.kafka.eventstreams.impl.listener;

import com.mapr.fs.jni.MapRUserInfo;
import com.mapr.fs.jni.NativeData;
import com.mapr.fs.proto.Dbserver.CDCOpenFormatType;
import com.mapr.fs.proto.Marlincommon.MarlinTimestampType;
import com.mapr.fs.proto.Marlinserver.MarlinConfigDefaults;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class MarlinListenerImplV10 extends MarlinListenerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(MarlinListenerImplV10.class);

    // MarlinListenerImplV10 specific config checks.
    protected void checkConsumerConfig(ConsumerConfig config) throws KafkaException {
      super.checkConsumerConfig(config);
      MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

      try {
        Integer maxPollRecords =
            config.getInt(mConfDef.getMaxPollRecords());
        if (maxPollRecords < 1) {
          throw new ConfigException(mConfDef.getFetchMinBytes() + " Cannot be less than 1");
        }
      } catch (ConfigException e) {
        LOG.error(mConfDef.getFetchMinBytes() + " configuration");
        throw e;
      }
    }

    @Override
    protected void createJniListener(ConsumerConfig config,
                                     boolean hardMount,
                                     MapRUserInfo userInfo,
                                     CDCOpenFormatType cdcOFType) throws NetworkException {

      MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();

      _clntPtr = OpenListener(config.getString(mConfDef.getClientID()),
              // TODO Marlin: core should be able to handle null group id as it is default value.
              // Currently JVM will just crush if you pass null here
              Optional.ofNullable(config.getString(mConfDef.getGroupID())).orElse(""),
          rpcTimeoutMs,
          hardMount,
          config.getBoolean(mConfDef.getAutoCommitEnabled()),
          config.getInt(mConfDef.getAutoCommitInterval()),
          autoCommitCbWrapper,
          config.getLong(mConfDef.getMetadataMaxAge()),
          config.getInt(mConfDef.getFetchMsgMaxBytesPerPartition()),
          config.getInt(mConfDef.getFetchMsgMaxBytes()),
          config.getInt(mConfDef.getFetchMinBytes()),
          config.getInt(mConfDef.getFetchMaxWaitMs()),
          config.getString(mConfDef.getAutoOffsetReset()),
          defaultStreamName,
          config.getLong(mConfDef.getConsumerBufferMemory()),
          config.getBoolean(mConfDef.getNegativeOffsetOnEof()),
          userInfo,
          cdcOFType.getNumber(),
          config.getInt(mConfDef.getMaxPollRecords()));

      if (_clntPtr == 0) {
        /**
         * <MAPR_ERROR>
         *   Unable to create MapRClient object for the cluster
         *   specified in hostname:port
         * </MAPR_ERROR>
         */
        throw new NetworkException("Could not create Consumer. Please ensure"
            + " that the CLDB service is configured"
            + " properly and is available");
      }
    }

    public MarlinListenerImplV10(ConsumerConfig config, ConsumerInterceptors<?, ?> interceptors,
                                 CDCOpenFormatType cdcOFType) {
      super(config, interceptors, cdcOFType);

      LOG.debug("MarlinListenerImplV10 constructor");
    }

    public TimestampType convertMarlinTimestampTypeToKafka(int ttype) {
      MarlinTimestampType implTtype = MarlinTimestampType.valueOf(ttype);
      switch (implTtype) {
      case CreateTime:
        return TimestampType.CREATE_TIME;
      case LogAppendTime:
        return TimestampType.LOG_APPEND_TIME;
      case NoTimestampType:
      default:
        return TimestampType.NO_TIMESTAMP_TYPE;
      }
    }

    @Override
    protected NativeDataParser getNativeDataParser(NativeData native_response) {
      return new NativeDataParserV10(native_response);
    }
}
