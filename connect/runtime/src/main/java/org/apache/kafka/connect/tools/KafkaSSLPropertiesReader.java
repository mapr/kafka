package org.apache.kafka.connect.tools;

import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;


public class KafkaSSLPropertiesReader {

  /**
   * Reads server keystore type.
   * @return server keystore type as string
   */

  public static String getServerKeystoreType() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      return sslConfig.getServerKeystoreType();
    }
  }

  /**
   * Reads server keystore location.
   * @return server keystore location as string
   */

  public static String getServerKeystoreLocation() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      return sslConfig.getServerKeystoreLocation();
    }
  }

  /**
   * Reads server keystore password value.
   * @return server keystore password value as string
   */

  public static String getServerKeystorePassword() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      return new String(sslConfig.getServerKeystorePassword());
    }
  }

  /**
   * Reads server key password value.
   * @return server key password value as string
   */

  public static String getServerKeyPassword() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      return new String(sslConfig.getServerKeyPassword());
    }
  }

  /**
   * Reads server truststore type.
   * @return server truststore type as string
   */

  public static String getServerTruststoreType() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      return sslConfig.getServerTruststoreType();
    }
  }
}