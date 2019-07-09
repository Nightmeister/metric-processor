package net.aivanov.metric.store;

import java.util.Collection;
import net.aivanov.metric.Metric;

/**
 * Metric store.
 *
 * @author Artem_Ivanov
 */
public interface MetricStore {

  /**
   * Store metric collection.
   *
   * @param metrics Metrics.
   */
  void store(Collection<Metric> metrics);

  /**
   * Sum of all values filtered by key and range.
   *
   * @param key Key.
   * @param startTimestampInclusive Start timestamp.
   * @param endTimestampExclusive End timestamp.
   * @return Sum.
   */
  long sumAllValuesByKeyAndRange(char key, long startTimestampInclusive, long endTimestampExclusive);
}
