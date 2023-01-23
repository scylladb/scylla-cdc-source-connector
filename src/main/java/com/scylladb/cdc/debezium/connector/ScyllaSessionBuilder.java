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
            sslBuilder.withSslProvider(configuration.getSslProvider());
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
        return new Driver3Session(builder.build());
    }
}