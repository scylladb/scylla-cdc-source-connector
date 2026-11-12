package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.flogger.backend.slf4j.Slf4jBackendFactory;
import com.google.common.flogger.backend.system.BackendFactory;
import com.google.common.flogger.util.StaticMethodCaller;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

@Isolated
class FloggerBackendFactoryTest {

  @Test
  void connectorSelectsSlf4jWithoutServiceLoader() throws Exception {
    assertEntrypointSelectsSlf4j("ScyllaConnector");
  }

  @Test
  void taskSelectsSlf4jWithoutServiceLoader() throws Exception {
    assertEntrypointSelectsSlf4j("ScyllaConnectorTask");
  }

  private void assertEntrypointSelectsSlf4j(String simpleName) throws Exception {
    String property = "flogger.backend_factory";
    String original = System.getProperty(property);
    String entrypoint = getClass().getPackageName() + "." + simpleName;
    URL classes = ScyllaConnector.class.getProtectionDomain().getCodeSource().getLocation();
    // Reload only the entrypoint so its static block runs even after other tests initialized it.
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {classes}, getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(entrypoint)) {
              return super.loadClass(name, resolve);
            }
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
              loaded = findClass(name);
            }
            if (resolve) {
              resolveClass(loaded);
            }
            return loaded;
          }
        }) {
      System.clearProperty(property);
      Class.forName(entrypoint, true, loader);
      // Exercise Flogger's property resolver directly: a missing or invalid property returns null
      // instead of falling back to the backend's service descriptor.
      BackendFactory factory =
          StaticMethodCaller.getInstanceFromSystemProperty(property, BackendFactory.class);
      assertNotNull(factory, "Entrypoint must configure a valid Flogger backend factory");
      assertInstanceOf(Slf4jBackendFactory.class, factory);
      assertEquals(
          "com.google.common.flogger.backend.slf4j.Slf4jLoggerBackend",
          factory.create(getClass().getName()).getClass().getName());
    } finally {
      if (original == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, original);
      }
    }
  }
}
