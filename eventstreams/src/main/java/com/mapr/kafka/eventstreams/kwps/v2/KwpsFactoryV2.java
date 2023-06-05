/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps.v2;

import java.io.IOException;

import com.mapr.fs.proto.Security.CredentialsMsg;
import com.mapr.kwps.KwpsFactory;
import com.mapr.kwps.KTopicsAdmin;

public class KwpsFactoryV2 implements KwpsFactory {

  public KTopicsAdmin newKafkaTopicsAdmin(CredentialsMsg userCredentials) throws IOException {
    return new KafkaTopicsAdminV2(userCredentials);
  }

  public String getName() {
    return "v2";
  }

}
