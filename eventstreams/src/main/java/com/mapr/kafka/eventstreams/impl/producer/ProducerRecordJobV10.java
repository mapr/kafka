package com.mapr.kafka.eventstreams.impl.producer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class ProducerRecordJobV10 extends ProducerRecordJob {
  private Headers headers;
  public ProducerRecordJobV10(MarlinProducerResultImpl res, byte[] k, byte[] v, long t, Headers headers) {
    super(res, k, v, t);
    this.headers = headers;
  }

  public ProducerRecordJobV10() {
    super();
    this.headers = null;
  }

  public Headers getHeaders() { return headers; }

  @Override
  public int estimatedSizeInBytes() {
    int headersSize = 0;
    for(Header header: headers.toArray())
      headersSize += header.key().getBytes().length + header.value().length;
    return super.estimatedSizeInBytes() + headersSize;
  }
}
