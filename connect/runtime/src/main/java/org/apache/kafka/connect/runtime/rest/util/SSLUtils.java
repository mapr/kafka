/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.rest.util;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Helper class for setting up SSL for RestServer and RestClient
 */
public class SSLUtils {
  private static final Logger log = LoggerFactory.getLogger(SSLUtils.class);

  /**
     * Configures SSL/TLS for HTTPS Jetty Server / Client
     */
    public static SslContextFactory createSslContextFactory(WorkerConfig config) {
        return createSslContextFactory(config, false);
    }

    /**
     * Configures SSL/TLS for HTTPS Jetty Server / Client
     */
    public static SslContextFactory createSslContextFactory(WorkerConfig config, boolean client) {
        Map<String, Object> sslConfigValues = config.valuesWithPrefixAllOrNothing("listeners.https.");

        SslContextFactory ssl = new SslContextFactory();

        configureSslContextFactoryKeyStore(config, ssl, sslConfigValues);
        configureSslContextFactoryTrustStore(ssl, sslConfigValues);
        configureSslContextFactoryAlgorithms(ssl, sslConfigValues);
        configureSslContextFactoryAuthentication(ssl, sslConfigValues);

        if (client)
            configureSslContextFactoryEndpointIdentification(ssl, sslConfigValues);

        return ssl;
    }

  private static String getFromCredentialsPath(String name, String credentialsPath){
    Method getPasswordMethod;

    try {
      // Only supported in Hadoop 2.6.0+
      getPasswordMethod = Configuration.class.getMethod("getPassword", String.class);
    } catch (NoSuchMethodException e) {
      // Not supported
      getPasswordMethod = null;
    }
    Configuration hadoopConf = new Configuration();

    if (getPasswordMethod != null) {
      hadoopConf.set("hadoop.security.credential.provider.path", credentialsPath);
      try {
        char[] pass = (char[]) getPasswordMethod.invoke(hadoopConf, name);
        return pass == null ? "" : new String(pass);
      } catch (IllegalAccessException e) {
        log.error("Could not load password " + e);
        throw new IllegalArgumentException("Could not load password for [" + name + "]", e);
      } catch (InvocationTargetException e) {
        log.error("Could not load password " + e);
        throw new IllegalArgumentException("Could not load password for [" + name + "]", e);
      }
    } else
      return "";
  }
    /**
     * Configures KeyStore related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryKeyStore(WorkerConfig config, SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        ssl.setKeyStoreType((String) getOrDefault(sslConfigValues, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE));

        String sslKeystoreLocation = (String) sslConfigValues.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (sslKeystoreLocation != null)
            ssl.setKeyStorePath(sslKeystoreLocation);
        String credentialsPath = config.getString(WorkerConfig.HADOOP_CREDENTIALS_PROVIDER_PATH);
        Password sslKeystorePassword = (Password) sslConfigValues.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        Password sslKeyPassword = (Password) sslConfigValues.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);

        if (!credentialsPath.isEmpty()) {
            String tmpKeystorePassword = getFromCredentialsPath((SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), credentialsPath);
            String tmpKeyPassword = getFromCredentialsPath((SslConfigs.SSL_KEY_PASSWORD_CONFIG), credentialsPath);

            if (!tmpKeystorePassword.isEmpty())
                sslKeystorePassword = new Password(tmpKeystorePassword);

            if (!tmpKeyPassword.isEmpty())
                sslKeyPassword = new Password(tmpKeyPassword);
        }

        if (sslKeystorePassword != null)
            ssl.setKeyStorePassword(sslKeystorePassword.value());

        if (sslKeyPassword != null)
            ssl.setKeyManagerPassword(sslKeyPassword.value());
    }

    protected static Object getOrDefault(Map<String, Object> configMap, String key, Object defaultValue) {
        if (configMap.containsKey(key))
            return configMap.get(key);

        return defaultValue;
    }

    /**
     * Configures TrustStore related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryTrustStore(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        ssl.setTrustStoreType((String) getOrDefault(sslConfigValues, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE));

        String sslTruststoreLocation = (String) sslConfigValues.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (sslTruststoreLocation != null)
            ssl.setTrustStorePath(sslTruststoreLocation);

        Password sslTruststorePassword = (Password) sslConfigValues.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        if (sslTruststorePassword != null)
            ssl.setTrustStorePassword(sslTruststorePassword.value());
    }

    /**
     * Configures Protocol, Algorithm and Provider related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryAlgorithms(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        List<String> sslEnabledProtocols = (List<String>) getOrDefault(sslConfigValues, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")));
        ssl.setIncludeProtocols(sslEnabledProtocols.toArray(new String[sslEnabledProtocols.size()]));

        String sslProvider = (String) sslConfigValues.get(SslConfigs.SSL_PROVIDER_CONFIG);
        if (sslProvider != null)
            ssl.setProvider(sslProvider);

        ssl.setProtocol((String) getOrDefault(sslConfigValues, SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL));

        List<String> sslCipherSuites = (List<String>) sslConfigValues.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (sslCipherSuites != null)
            ssl.setIncludeCipherSuites(sslCipherSuites.toArray(new String[sslCipherSuites.size()]));

        ssl.setSslKeyManagerFactoryAlgorithm((String) getOrDefault(sslConfigValues, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM));

        String sslSecureRandomImpl = (String) sslConfigValues.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (sslSecureRandomImpl != null)
            ssl.setSecureRandomAlgorithm(sslSecureRandomImpl);

        ssl.setTrustManagerFactoryAlgorithm((String) getOrDefault(sslConfigValues, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM));
    }

    /**
     * Configures Protocol, Algorithm and Provider related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryEndpointIdentification(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        String sslEndpointIdentificationAlg = (String) sslConfigValues.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        if (sslEndpointIdentificationAlg != null)
            ssl.setEndpointIdentificationAlgorithm(sslEndpointIdentificationAlg);
    }

    /**
     * Configures Authentication related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryAuthentication(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        String sslClientAuth = (String) getOrDefault(sslConfigValues, BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        switch (sslClientAuth) {
            case "requested":
                ssl.setWantClientAuth(true);
                break;
            case "required":
                ssl.setNeedClientAuth(true);
                break;
            default:
                ssl.setNeedClientAuth(false);
                ssl.setWantClientAuth(false);
        }
    }
}
