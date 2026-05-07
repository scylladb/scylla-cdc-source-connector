package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

  private static long countOpenFileDescriptors() throws IOException {
    Path fdDir = Path.of("/proc/self/fd");
    try (Stream<Path> entries = Files.list(fdDir)) {
      return entries.count();
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

    long before = countOpenFileDescriptors();
    for (int i = 0; i < 10; i++) {
      assertNotNull(invokeBuildSslOptionsSuccessfully(checker));
    }
    long after = countOpenFileDescriptors();

    assertTrue(
        after - before <= 2,
        "Expected PEM input streams to be closed, but open FDs grew from "
            + before
            + " to "
            + after);
  }
}
