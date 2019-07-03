package org.apache.kafka.streams.examples;

public class MaprConfig {
    /**
     * To create MapR stream run:
     * `$ maprcli stream create -path /kstreams-examples`
     *
     * To create topic run:
     * `$ maprcli stream topic create -path /kstreams-examples -topic <name>`
     */
    public static final String STREAM_NAME = "/kstreams-examples";
}
