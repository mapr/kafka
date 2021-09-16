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
}