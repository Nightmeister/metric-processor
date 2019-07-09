package net.aivanov.metric.processor;

/**
 * Metric processor.
 *
 * @author Artem_Ivanov
 */
public interface MetricProcessor {

  /**
   * Add metric.
   *
   * @param timestamp Timestamp.
   * @param key Key.
   * @param value Value.
   */
  void add(long timestamp, char key, int value);

  /**
   * Calculate metric sum by params.
   *
   * @param startTimestampInclusive Start timestamp.
   * @param endTimestampExclusive End timestamp.
   * @param key Key.
   * @return Result.
   */
  long sum(long startTimestampInclusive, long endTimestampExclusive, char key);
}
