/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The future result that is returned to the producer on a send call. The
 * future is activated when the marlin server actually responds to the send
 * request with a callback.
 */
public final class MarlinFuture implements Future<RecordMetadata> {

    private final MarlinProducerResultImpl result;

    public MarlinFuture(MarlinProducerResultImpl result) {
        this.result = result;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        return valueOrError();
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + TimeUnit.MILLISECONDS.convert(timeout, unit) + " ms.");
        return valueOrError();
    }

    RecordMetadata valueOrError() throws ExecutionException {
        if (this.result.error() != null)
            throw new ExecutionException(this.result.error());
        else
            return value();
    }
    
    RecordMetadata value() {
      return this.result.getRecordMetadata();
    }
    

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return this.result.completed();
    }

}
