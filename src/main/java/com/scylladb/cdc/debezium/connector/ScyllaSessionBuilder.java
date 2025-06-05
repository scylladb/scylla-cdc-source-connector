package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.SslConfig;
import com.scylladb.cdc.cql.driver3.Driver3Session;

public class ScyllaSessionBuilder {
    private final ScyllaConnectorConfig configuration;

    public ScyllaSessionBuilder(ScyllaConnectorConfig configuration) {
        this.configuration = configuration;
    }

    public Driver3Session build() {
        CQLConfiguration.Builder builder = CQLConfiguration.builder();
        builder.withDefaultPort(configuration.getDefaultPort());
        builder.addContactPoints(configuration.getContactPoints());
        if (configuration.getUser() != null && configuration.getPassword() != null) {
            builder.withCredentials(configuration.getUser(), configuration.getPassword());
        }
        builder.withConsistencyLevel(configuration.getConsistencyLevel());
        if (configuration.getLocalDCName() != null) {
            builder.withLocalDCName(configuration.getLocalDCName());
        }
        if (configuration.getSslEnabled()) {
            SslConfig.Builder sslBuilder = SslConfig.builder();
            sslBuilder.withSslProviderString(configuration.getSslProvider().toString());
            sslBuilder.withTrustStorePath(configuration.getTrustStorePath());
            sslBuilder.withTrustStorePassword(configuration.getTrustStorePassword());
            sslBuilder.withKeyStorePath(configuration.getKeyStorePath());
            sslBuilder.withKeyStorePassword(configuration.getKeyStorePassword());
            if (configuration.getCipherSuite() != null) {
                configuration.getCipherSuite().stream().forEach(sslBuilder::withCipher);
            }
            sslBuilder.withCertPath(configuration.getCertPath());
            sslBuilder.withPrivateKeyPath(configuration.getPrivateKeyPath());
            builder.withSslConfig(sslBuilder.build());
        }
        builder.withQueryOptionsFetchSize(configuration.getQueryOptionsFetchSize());
        builder.withCorePoolLocal(configuration.getPoolingCorePoolLocal());
        builder.withMaxPoolLocal(configuration.getPoolingMaxPoolLocal());
        builder.withPoolingMaxRequestsPerConnectionLocal(configuration.getPoolingMaxRequestsPerConnection());
        builder.withPoolingMaxQueueSize(configuration.getPoolingMaxQueueSize());
        builder.withPoolTimeoutMillis(configuration.getPoolingPoolTimeoutMs());

        return new Driver3Session(builder.build());
    }
}