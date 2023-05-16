package com.mapr.kafka.eventstreams;

import java.util.NoSuchElementException;

/**
 * The timestamp type of the records
 */
public enum TimestampType {
    CREATE_TIME(0, "CreateTime"), LOG_APPEND_TIME(1, "LogAppendTime");

    public final int id;
    public final String name;

    TimestampType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static TimestampType forName(String name) {
        for (TimestampType t : values())
            if (t.name.equalsIgnoreCase(name))
                return t;
        throw new NoSuchElementException("Invalid timestamp type " + name);
    }

    @Override
    public String toString() {
        return name;
    }

}
