package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import io.debezium.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link ScyllaVersionChecker}. */
public class ScyllaVersionCheckerTest {

  private ScyllaConnectorConfig createSslConfig(
      String certPath, String privateKeyPath, String trustStorePath, String keyStorePath) {
    Configuration.Builder builder =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.table")
            .with("scylla.ssl.enabled", "true")
            .with("scylla.ssl.provider", "JDK");

    if (certPath != null) {
      builder.with("scylla.ssl.openssl.keyCertChain", certPath);
    }
    if (privateKeyPath != null) {
      builder.with("scylla.ssl.openssl.privateKey", privateKeyPath);
    }
    if (trustStorePath != null) {
      builder.with("scylla.ssl.truststore.path", trustStorePath);
    }
    if (keyStorePath != null) {
      builder.with("scylla.ssl.keystore.path", keyStorePath);
    }

    return new ScyllaConnectorConfig(builder.build());
  }

  private InvocationTargetException invokeBuildSslOptionsExpectingFailure(
      ScyllaVersionChecker checker) throws Exception {
    Method method = ScyllaVersionChecker.class.getDeclaredMethod("buildSslOptions");
    method.setAccessible(true);
    return assertThrows(InvocationTargetException.class, () -> method.invoke(checker));
  }

  private Object invokeBuildSslOptionsSuccessfully(ScyllaVersionChecker checker) throws Exception {
    Method method = ScyllaVersionChecker.class.getDeclaredMethod("buildSslOptions");
    method.setAccessible(true);
    return method.invoke(checker);
  }

  private static void assertNoOpenFileDescriptors(Path file) throws IOException {
    Path realPath = file.toRealPath();
    try (Stream<Path> entries = Files.list(Path.of("/proc/self/fd"))) {
      for (Path fd : entries.toList()) {
        Path target;
        try {
          target = Files.readSymbolicLink(fd);
        } catch (NoSuchFileException e) {
          // Other tests may close unrelated descriptors while this test scans them.
          continue;
        }
        assertNotEquals(realPath, target, "Expected PEM input stream to be closed: " + file);
      }
    }
  }

  private static void generatePemPair(Path certPath, Path privateKeyPath)
      throws IOException, InterruptedException {
    Process process =
        new ProcessBuilder(
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                privateKeyPath.toString(),
                "-out",
                certPath.toString(),
                "-subj",
                "/CN=localhost",
                "-days",
                "1",
                "-sha256")
            .redirectErrorStream(true)
            .start();

    String output;
    try (InputStream inputStream = process.getInputStream()) {
      output = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    int exitCode = process.waitFor();
    assertTrue(exitCode == 0, "openssl failed with exit code " + exitCode + ": " + output);
  }

  @Test
  void buildSslOptions_requiresPrivateKeyWhenOpenSslCertChainIsConfigured() throws Exception {
    ScyllaVersionChecker checker =
        new ScyllaVersionChecker(
            createSslConfig("/does/not/exist/client-cert.pem", null, null, null));

    InvocationTargetException ex = invokeBuildSslOptionsExpectingFailure(checker);
    assertTrue(ex.getCause() instanceof RuntimeException);
    assertTrue(ex.getCause().getMessage().contains("scylla.ssl.openssl.privateKey"));
  }

  @Test
  void buildSslOptions_requiresCertChainWhenOpenSslPrivateKeyIsConfigured() throws Exception {
    ScyllaVersionChecker checker =
        new ScyllaVersionChecker(
            createSslConfig(null, "/does/not/exist/client-key.pem", null, null));

    InvocationTargetException ex = invokeBuildSslOptionsExpectingFailure(checker);
    assertTrue(ex.getCause() instanceof RuntimeException);
    assertTrue(ex.getCause().getMessage().contains("scylla.ssl.openssl.keyCertChain"));
  }

  @Test
  void buildSslOptions_closesPemInputStreams(@TempDir Path tempDir) throws Exception {
    Assumptions.assumeTrue(Files.isDirectory(Path.of("/proc/self/fd")));

    Path certPath = tempDir.resolve("client-cert.pem");
    Path privateKeyPath = tempDir.resolve("client-key.pem");
    generatePemPair(certPath, privateKeyPath);

    ScyllaVersionChecker checker =
        new ScyllaVersionChecker(
            createSslConfig(certPath.toString(), privateKeyPath.toString(), null, null));

    for (int i = 0; i < 10; i++) {
      assertNotNull(invokeBuildSslOptionsSuccessfully(checker));
      assertNoOpenFileDescriptors(certPath);
      assertNoOpenFileDescriptors(privateKeyPath);
    }
  }

  private static Session sessionReturning(Row row) {
    Session session = mock(Session.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(session.execute(ScyllaVersionChecker.VERSION_QUERY)).thenReturn(resultSet);
    when(resultSet.one()).thenReturn(row);
    return session;
  }

  private static Row versionRow(String version) {
    Row row = mock(Row.class);
    when(row.getString("version")).thenReturn(version);
    return row;
  }

  @Test
  void queryVersion_parsesSystemVersions() {
    Session session = sessionReturning(versionRow("2026.1.14-0.20260924.0dbea6201d67"));
    assertEquals(new ScyllaVersion(2026, 1, 14), ScyllaVersionChecker.queryVersion(session));
  }

  @Test
  void queryVersion_returnsNullWhenRowMissing() {
    assertNull(ScyllaVersionChecker.queryVersion(sessionReturning(null)));
  }

  @Test
  void queryVersion_returnsNullWhenUnparseable() {
    assertNull(ScyllaVersionChecker.queryVersion(sessionReturning(versionRow("unknown"))));
  }

  private static AddressTranslator addressTranslator(String translator) {
    Configuration.Builder builder =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.table");
    if (translator != null) {
      builder.with("scylla.address.translator", translator);
    }
    return new ScyllaVersionChecker(new ScyllaConnectorConfig(builder.build()))
        .newClusterBuilder()
        .getConfiguration()
        .getPolicies()
        .getAddressTranslator();
  }

  @Test
  void newClusterBuilder_appliesEc2MultiRegionAddressTranslator() {
    assertInstanceOf(EC2MultiRegionAddressTranslator.class, addressTranslator("EC2_MULTI_REGION"));
  }

  @Test
  void newClusterBuilder_keepsDefaultAddressTranslator() {
    assertFalse(addressTranslator(null) instanceof EC2MultiRegionAddressTranslator);
  }
}
