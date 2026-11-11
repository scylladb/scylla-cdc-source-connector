package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.backend.Platform;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class FloggerBackendTest {

  @Test
  void discoveredSlf4jBackendEmitsMessagesAndCauses() {
    assertEquals(
        "com.google.common.flogger.backend.slf4j.Slf4jLoggerBackend",
        Platform.getBackend(FloggerBackendTest.class.getName()).getClass().getName());
    Logger logger = Logger.getLogger(FloggerBackendTest.class.getName());
    Level originalLevel = logger.getLevel();
    AtomicReference<LogRecord> captured = new AtomicReference<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            captured.set(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    logger.setLevel(Level.WARNING);
    try {
      RuntimeException cause = new RuntimeException("test cause");
      FluentLogger.forEnclosingClass().atWarning().withCause(cause).log("Flogger %s", "message");
      LogRecord event = captured.get();
      assertNotNull(event);
      assertEquals("Flogger message", event.getMessage());
      assertEquals(Level.WARNING, event.getLevel());
      assertSame(cause, event.getThrown());
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      handler.close();
    }
  }
}
