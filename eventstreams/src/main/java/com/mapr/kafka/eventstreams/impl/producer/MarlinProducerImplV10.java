package com.mapr.kafka.eventstreams.impl.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class MarlinProducerImplV10 extends MarlinProducerImpl {
  private Headers headers;

  public MarlinProducerImplV10(ProducerConfig config) throws KafkaException {
    super(config);
  }

  @Override
  protected RecordMetadata getDummyRecordMetadata(String topic) {
    return new RecordMetadata(new TopicPartition(topic, -1), -1, 0, -1L,  -1, -1);
  }

  @Override
  protected MarlinProducerResultImpl getMarlinProducerResultImpl(String topic, int feed, Callback callback,
                                                                  int serKeySz, int serValSz) {
    return new MarlinProducerResultImplV10(topic, feed, callback, serKeySz, serValSz);
  }

  @Override
  public <K, V> Future<RecordMetadata> send(ProducerRecord<K, V> record, int feed,
                                            byte[] serializedKey, byte[] serializedValue,
                                            Callback callback) throws KafkaException {
    return send(record.topic(), feed, record.key(), serializedKey,
                record.value(), serializedValue, callback,
                record.timestamp() != null ?
                  record.timestamp() : marlinInternalDefaults.getNoTimestamp(),
                record.headers());
  }

  public Future<RecordMetadata> send(String topic, int feed, Object keyObj, byte[] key,
      Object valueObj, byte[] value,
      Callback callback, long timestamp, Headers headers) throws KafkaException {

    this.headers = headers;
    setReadOnly(headers);

    return super.do_send(topic, feed, keyObj, key, valueObj, value, callback, timestamp);

  }

  private void setReadOnly(Headers headers) {
    if (headers instanceof RecordHeaders) {
      ((RecordHeaders) headers).setReadOnly();
    }
  }

  @Override
  protected ProducerRecordJob getProducerRecordJob(MarlinProducerResultImpl result, byte[] key,  byte[] value, long timestamp) {
    return new ProducerRecordJobV10(result, key, value, timestamp, headers);
  }

  @Override
  protected ProducerRecordJob getProducerRecordJob() {
    return new ProducerRecordJobV10();
  }

  /*
   * @brief Encode the jni send buffers based on kafka-1.0.1 records.
   * @return Number of messages to be sent.
   */
  @Override
  protected int encodeJniData(WorkerThread.WorkerState ws, int recSz) {
    int numOffsets = recSz * 3; // Topic name / key / val
    ws.results = new MarlinProducerResultImpl[recSz];
    ws.byteSize = 0;

    // Get number of offsets neeeded to represent record info.
    Header[][] headers = new Header[recSz][];
    for (int i = 0; i < recSz; i++) {
      ProducerRecordJobV10 rec = (ProducerRecordJobV10)ws.recList[i];
      if (rec.getHeaders() != null) {
        headers[i] = rec.getHeaders().toArray();
        numOffsets += (headers[i].length * 2); // For header key/val
      }
    }
    ws.byteOffsets = new int[numOffsets];

    int byteOffsetIndex = 0;
    int numMsgJobs = 0;
    for (int i = 0; i < recSz; ++i) {
      ProducerRecordJob rec = ws.recList[i];
      if (rec.isFlushJob()) {
        ws.flushJobs.add(rec);
        continue;
      }
      ws.feedIDs[numMsgJobs] = rec.getResult().getFeed();
      ws.results[numMsgJobs] = rec.getResult();
      ws.byteSize += rec.getTopic().length;
      ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      if (rec.getKey() != null)
        ws.byteSize += rec.getKey().length;
      ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      if (rec.getValue() != null)
        ws.byteSize += rec.getValue().length;
      ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      ws.timestamps[numMsgJobs] = rec.getTimestamp();

      if (headers[numMsgJobs] != null) {
        ws.numHeaders[numMsgJobs] = headers[numMsgJobs].length;
      } else {
        ws.numHeaders[numMsgJobs] = 0;
      }
      for (int j=0; j < ws.numHeaders[numMsgJobs]; j++) {
        // Header key can't be null
        assert(headers[numMsgJobs][j].key() != null);
        // Header key
        ws.byteSize += headers[numMsgJobs][j].key().getBytes().length;
        ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;

        // Header value
        ws.byteSize += (headers[numMsgJobs][j].value() == null) ? 0 :
          headers[numMsgJobs][j].value().length;
        ws.byteOffsets[byteOffsetIndex++] = ws.byteSize;
      }
      numMsgJobs++;
    }

    if (ws.maxBytesToSend < ws.byteSize){
      LOG.debug("Worker thread increasing byte array from " + ws.maxBytesToSend + " to " + ws.byteSize);
      ws.toSend = new byte[ws.byteSize];
      ws.maxBytesToSend = ws.byteSize;
    }

    ByteBuffer toSendBuffer = ByteBuffer.wrap(ws.toSend);
    numMsgJobs = 0;
    for (int i = 0; i < recSz; ++i) {
      ProducerRecordJob rec = ws.recList[i];
      if (rec.isFlushJob()) {
        continue;
      }

      toSendBuffer.put(rec.getTopic());
      if (rec.getKey() != null)
        toSendBuffer.put(rec.getKey());
      if (rec.getValue() != null)
        toSendBuffer.put(rec.getValue());
      for (int j=0; j < ws.numHeaders[numMsgJobs]; j++) {
        // Header key can't be null
        assert(headers[numMsgJobs][j].key() != null);
        toSendBuffer.put(headers[numMsgJobs][j].key().getBytes());

        if (headers[numMsgJobs][j].value() != null) {
          toSendBuffer.put(headers[numMsgJobs][j].value());
        }
      }
      numMsgJobs++;
    }

    return numMsgJobs;

  }
}