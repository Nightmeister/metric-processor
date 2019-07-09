package net.aivanov.metric;

import net.aivanov.metric.processor.NaiveMetricProcessor;
import net.aivanov.metric.store.FileMetricStore;
import net.aivanov.metric.store.MetricStore;

/**
 * Application class.
 *
 * @author Artem_Ivanov
 */
public class Application {

  /**
   * Main point.
   */
  public static void main(String[] args) {
    MetricStore metricStore = new FileMetricStore("/Users/artyom.ivanov/Documents/Trash/store");
    NaiveMetricProcessor metricProcessor = new NaiveMetricProcessor(metricStore);

    metricProcessor.add(0, 'a', 1);
    metricProcessor.add(1, 'b', 1);
    metricProcessor.add(2, 'a', -3);

    System.out.println(metricProcessor.sum(0, 3, 'a')); // -2
    System.out.println(metricProcessor.sum(2, 3, 'b')); // 0
    System.out.println(metricProcessor.sum(0, 1, 'b')); // 0

    System.exit(0);
  }
}
