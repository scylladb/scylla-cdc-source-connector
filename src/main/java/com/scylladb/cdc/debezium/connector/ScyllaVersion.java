package com.scylladb.cdc.debezium.connector;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Scylla version and provides comparison functionality.
 *
 * <p>Scylla versions follow the format: major.minor.patch (e.g., "2026.1.0", "5.4.2").
 */
public class ScyllaVersion implements Comparable<ScyllaVersion> {

  /**
   * Minimum version that supports preimage for partition deletes on tables without clustering key.
   */
  public static final ScyllaVersion PARTITION_DELETE_PREIMAGE_SUPPORT =
      new ScyllaVersion(2026, 1, 0);

  private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

  private final int major;
  private final int minor;
  private final int patch;

  public ScyllaVersion(int major, int minor, int patch) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  /**
   * Parses a version string into a ScyllaVersion.
   *
   * @param versionString the version string (e.g., "2026.1.0" or "5.4.2-0.20240101.abc123")
   * @return the parsed ScyllaVersion, or null if parsing fails
   */
  public static ScyllaVersion parse(String versionString) {
    if (versionString == null || versionString.isEmpty()) {
      return null;
    }

    Matcher matcher = VERSION_PATTERN.matcher(versionString);
    if (matcher.find()) {
      try {
        int major = Integer.parseInt(matcher.group(1));
        int minor = Integer.parseInt(matcher.group(2));
        int patch = Integer.parseInt(matcher.group(3));
        return new ScyllaVersion(major, minor, patch);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getPatch() {
    return patch;
  }

  /**
   * Checks if this version is greater than or equal to the other version.
   *
   * @param other the version to compare against
   * @return true if this version >= other version
   */
  public boolean isAtLeast(ScyllaVersion other) {
    return this.compareTo(other) >= 0;
  }

  /**
   * Checks if this version supports preimage for partition deletes on tables without clustering
   * key.
   *
   * @return true if version >= 2026.1.0
   */
  public boolean supportsPartitionDeletePreimage() {
    return isAtLeast(PARTITION_DELETE_PREIMAGE_SUPPORT);
  }

  @Override
  public int compareTo(ScyllaVersion other) {
    if (this.major != other.major) {
      return Integer.compare(this.major, other.major);
    }
    if (this.minor != other.minor) {
      return Integer.compare(this.minor, other.minor);
    }
    return Integer.compare(this.patch, other.patch);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScyllaVersion that = (ScyllaVersion) o;
    return major == that.major && minor == that.minor && patch == that.patch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, patch);
  }

  @Override
  public String toString() {
    return major + "." + minor + "." + patch;
  }
}
