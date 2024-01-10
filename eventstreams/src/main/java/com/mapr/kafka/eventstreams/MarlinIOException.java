/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams;

import java.io.IOException;

public class MarlinIOException extends IOException {

  private static final long serialVersionUID = 1L;
  private int errorCode = -1;

  public MarlinIOException() {
  }

  public MarlinIOException(String message) {
    super(message);
  }

  public MarlinIOException(Throwable cause) {
    super(cause);
  }

  public MarlinIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public int getErrorCode() {
    return errorCode;
  }

  public MarlinIOException setErrorCode(int errorCode) {
    if (errorCode != -1) {
      this.errorCode = Math.abs(errorCode);
    }
    return this;
  }

}
