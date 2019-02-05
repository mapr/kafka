package org.apache.kafka.connect.runtime.rest.errors;

import org.apache.kafka.connect.errors.ConnectException;

public class SSLConfigException extends ConnectException {

    public SSLConfigException(String propertyName) {
        super(String.format("Property %s missed in ssl-client.xml file." +
                " This property is required in the secure mode.", propertyName));
    }
}
