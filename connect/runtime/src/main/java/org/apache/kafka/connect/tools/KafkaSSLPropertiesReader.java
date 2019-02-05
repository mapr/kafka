package org.apache.kafka.connect.tools;

import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;


public class KafkaSSLPropertiesReader {

  /**
   * Reads client keystore location.
   * @return client keystore location as string
   */

  public static String getClientKeystoreLocation() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig(SslConfig.SslConfigScope.SCOPE_CLIENT_ONLY)) {
      return sslConfig.getClientKeystoreLocation();
    }
  }

  /**
   * Reads client keystore password value.
   * @return client keystore password value as string
   */

  public static String getClientKeystorePassword() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig(SslConfig.SslConfigScope.SCOPE_CLIENT_ONLY)) {
      return new String(sslConfig.getClientKeystorePassword());

    }
  }

  /**
   * Reads client key password value.
   * @return client key password value as string
   */

  public static String getClientKeyPassword() {
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig(SslConfig.SslConfigScope.SCOPE_CLIENT_ONLY)) {
      return new String(sslConfig.getClientKeyPassword());

    }
  }
}