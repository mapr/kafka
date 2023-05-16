package com.mapr.kafka.eventstreams;

import com.mapr.fs.proto.Dbserver.CDCOpenFormatType;

public interface MapRCDCDeserializer {

    /**
     * @return the type of data the deserializer wants
     */
    CDCOpenFormatType getOpenFormatType();
}
