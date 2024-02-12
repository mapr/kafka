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

import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.security.Security;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper class for setting up SSL for {@link RestServer} and {@link RestClient}
 */
public class SSLUtils {

    private static final Map<String, Object> MAPR_SSL_CONFIGS = new HashMap<>();
    static {
        try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslConfig.getServerKeystoreType());
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslConfig.getServerKeystoreLocation());
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, new Password(new String(sslConfig.getServerKeystorePassword())));
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslConfig.getServerTruststoreType());
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.getServerTruststoreLocation());
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, new Password(new String(sslConfig.getServerTruststorePassword())));
            MAPR_SSL_CONFIGS.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password(new String(sslConfig.getServerKeyPassword())));
        }
    }

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");


    /**
     * Configures SSL/TLS for HTTPS Jetty Server using configs with the given prefix
     */
    public static SslContextFactory createServerSideSslContextFactory(AbstractConfig config, String prefix) {
        Map<String, Object> sslConfigValues = config.valuesWithPrefixAllOrNothing(prefix);
        MAPR_SSL_CONFIGS.forEach(sslConfigValues::putIfAbsent);

        final SslContextFactory.Server ssl = new SslContextFactory.Server();

        configureSslContextFactoryKeyStore(ssl, sslConfigValues);
        configureSslContextFactoryTrustStore(ssl, sslConfigValues);
        configureSslContextFactoryAlgorithms(ssl, sslConfigValues);
        configureSslContextFactoryAuthentication(ssl, sslConfigValues);

        return ssl;
    }

    /**
     * Configures SSL/TLS for HTTPS Jetty Server
     */
    public static SslContextFactory createServerSideSslContextFactory(AbstractConfig config) {
        return createServerSideSslContextFactory(config, "listeners.https.");
    }

    /**
     * Configures SSL/TLS for HTTPS Jetty Client
     */
    public static SslContextFactory createClientSideSslContextFactory(AbstractConfig config) {
        Map<String, Object> sslConfigValues = config.valuesWithPrefixAllOrNothing("listeners.https.");
        MAPR_SSL_CONFIGS.forEach(sslConfigValues::putIfAbsent);

        final SslContextFactory.Client ssl = new SslContextFactory.Client();

        configureSslContextFactoryKeyStore(ssl, sslConfigValues);
        configureSslContextFactoryTrustStore(ssl, sslConfigValues);
        configureSslContextFactoryAlgorithms(ssl, sslConfigValues);
        configureSslContextFactoryEndpointIdentification(ssl, sslConfigValues);

        return ssl;
    }

    private static void maybeEnableFips(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        String keyStoreType = (String) getOrDefault(sslConfigValues,
                SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE);
        if ("BCFKS".equals(keyStoreType)) {
            Security.addProvider(new BouncyCastleFipsProvider());
            Security.addProvider(new BouncyCastleJsseProvider());
            ssl.setProvider(BouncyCastleJsseProvider.PROVIDER_NAME);
        }
    }

    /**
     * Configures KeyStore related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryKeyStore(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        maybeEnableFips(ssl, sslConfigValues);
        ssl.setKeyStoreType((String) getOrDefault(sslConfigValues, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE));

        String sslKeystoreLocation = (String) sslConfigValues.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (sslKeystoreLocation != null)
            ssl.setKeyStorePath(sslKeystoreLocation);

        Password sslKeystorePassword = (Password) sslConfigValues.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        if (sslKeystorePassword != null)
            ssl.setKeyStorePassword(sslKeystorePassword.value());

        Password sslKeyPassword = (Password) sslConfigValues.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
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
    @SuppressWarnings("unchecked")
    protected static void configureSslContextFactoryAlgorithms(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        List<String> sslEnabledProtocols = (List<String>) getOrDefault(sslConfigValues, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList(COMMA_WITH_WHITESPACE.split(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS)));
        ssl.setIncludeProtocols(sslEnabledProtocols.toArray(new String[0]));

        List<String> sslDisabledProtocols = (List<String>) getOrDefault(sslConfigValues, RestServerConfig.SSL_DISABLED_PROTOCOLS_CONFIG, Arrays.asList(COMMA_WITH_WHITESPACE.split(RestServerConfig.SSL_DISABLED_PROTOCOLS_DEFAULT)));
        ssl.setExcludeProtocols(sslDisabledProtocols.toArray(new String[sslDisabledProtocols.size()]));

        String sslProvider = (String) sslConfigValues.get(SslConfigs.SSL_PROVIDER_CONFIG);
        if (sslProvider != null)
            ssl.setProvider(sslProvider);

        ssl.setProtocol((String) getOrDefault(sslConfigValues, SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL));

        List<String> sslCipherSuites = (List<String>) sslConfigValues.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (sslCipherSuites != null)
            ssl.setIncludeCipherSuites(sslCipherSuites.toArray(new String[0]));

        List<String> sslCipherSuitesExclude = (List<String>) getOrDefault(sslConfigValues, RestServerConfig.SSL_DISABLED_CIPHER_SUITES_CONFIG, Arrays.asList(COMMA_WITH_WHITESPACE.split(RestServerConfig.SSL_DISABLED_CIPHER_SUITES_DEFAULT)));
        ssl.setExcludeCipherSuites(sslCipherSuitesExclude.toArray(new String[sslCipherSuitesExclude.size()]));

        ssl.setKeyManagerFactoryAlgorithm((String) getOrDefault(sslConfigValues, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM));

        String sslSecureRandomImpl = (String) sslConfigValues.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (sslSecureRandomImpl != null)
            ssl.setSecureRandomAlgorithm(sslSecureRandomImpl);

        ssl.setTrustManagerFactoryAlgorithm((String) getOrDefault(sslConfigValues, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM));
    }

    /**
     * Configures hostname verification related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryEndpointIdentification(SslContextFactory ssl, Map<String, Object> sslConfigValues) {
        String sslEndpointIdentificationAlg = (String) sslConfigValues.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        if (sslEndpointIdentificationAlg != null)
            ssl.setEndpointIdentificationAlgorithm(sslEndpointIdentificationAlg);
    }

    /**
     * Configures Authentication related settings in SslContextFactory
     */
    protected static void configureSslContextFactoryAuthentication(SslContextFactory.Server ssl, Map<String, Object> sslConfigValues) {
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
