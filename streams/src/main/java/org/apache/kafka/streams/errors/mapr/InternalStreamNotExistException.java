package org.apache.kafka.streams.errors.mapr;

import org.apache.kafka.streams.errors.StreamsException;

public class InternalStreamNotExistException extends StreamsException {

    private static final long serialVersionUID = 1L;

    public InternalStreamNotExistException(final String internalStreamName) {
        super(composeMessage(internalStreamName));
    }

    public InternalStreamNotExistException(final String internalStreamName, final Throwable throwable) {
        super(composeMessage(internalStreamName), throwable);
    }

    public InternalStreamNotExistException(final Throwable throwable) {
        super(throwable);
    }

    private static String composeMessage(final String internalStreamName){
        return "Stream: '" + internalStreamName + "' does not exist. \n" +
        "KStream requires internal stream: '" + internalStreamName + "' to be created.";
    }
}