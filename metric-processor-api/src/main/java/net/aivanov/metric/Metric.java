package net.aivanov.metric;

import java.util.Objects;

/**
 * Metric data class.
 *
 * @author Artem_Ivanov
 */
public class Metric {

  /**
   * Key.
   */
  private final char key;

  /**
   * Value.
   */
  private final int value;

  /**
   * Timestamp.
   */
  private final long timestamp;

  public char getKey() {
    return key;
  }

  public int getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Metric(char key, int value, long timestamp) {
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Metric metric = (Metric) o;
    return key == metric.key &&
        value == metric.value &&
        timestamp == metric.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, timestamp);
  }
}
