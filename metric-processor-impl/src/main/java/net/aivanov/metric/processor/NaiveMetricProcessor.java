package net.aivanov.metric.processor;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import net.aivanov.metric.Metric;
import net.aivanov.metric.store.MetricStore;

/**
 * Naive metric processor implementation.
 *
 * @author Artem_Ivanov
 */
public class NaiveMetricProcessor implements MetricProcessor {

  /**
   * Metric queue capacity.
   */
  private final static int METRIC_QUEUE_CAPACITY = 10000000;

  /**
   * Metric queue comparator
   */
  private final static Comparator<Metric> METRIC_QUEUE_COMPARATOR =
      Comparator.<Metric>comparingLong(Metric::getTimestamp);

  /**
   * Metric queue.
   */
  private final BlockingQueue<Metric> metrics =
      new PriorityBlockingQueue<>(METRIC_QUEUE_CAPACITY, METRIC_QUEUE_COMPARATOR);

  /**
   * Metric store.
   */
  private final MetricStore metricStore;

  /**
   * Metric queue unloader.
   */
  private final MetricQueueStoreUnloader metricQueueStoreUnloader;

  public NaiveMetricProcessor(MetricStore metricStore) {
    this.metricStore = metricStore;
    this.metricQueueStoreUnloader = new MetricQueueStoreUnloader(metrics, metricStore);

    metricQueueStoreUnloader.start();
    Runtime.getRuntime().addShutdownHook(new Thread(metricQueueStoreUnloader::stop));
  }

  @Override
  public void add(long timestamp, char key, int value) {
    metrics.add(new Metric(key, value, timestamp));
  }

  @Override
  public long sum(long startTimestampInclusive, long endTimestampExclusive, char key) {
    return metricStore.sumAllValuesByKeyAndRange(key, startTimestampInclusive, endTimestampExclusive);
  }

  /**
   * Metric queue store unloader.
   */
  static class MetricQueueStoreUnloader {

    /**
     * Unload count.
     */
    private final static int UNLOAD_COUNT = 100000;

    /**
     * Metric store.
     */
    private final MetricStore metricStore;

    /**
     * Metric queue.
     */
    private final BlockingQueue<Metric> metricQueue;

    /**
     * Unload executor.
     */
    private final ExecutorService unloadExecutor = Executors.newFixedThreadPool(1);

    MetricQueueStoreUnloader(BlockingQueue<Metric> metricQueue, MetricStore metricStore) {
      this.metricStore = metricStore;
      this.metricQueue = metricQueue;
    }

    /**
     * Start unloader.
     */
    private void start() {
      unloadExecutor.execute(this::loopPollMetricsFromQueue);
    }

    /**
     * Stop unloader.
     */
    private void stop() {
      unloadExecutor.shutdown();
    }

    /**
     * Pool metrics from queue.
     *
     */
    private void loopPollMetricsFromQueue() {
      Queue<Metric> metricsToStore = new PriorityQueue<>(UNLOAD_COUNT, METRIC_QUEUE_COMPARATOR);

      while (true) {
        Metric metric = waitAvailableMetric();

        long polledCount = 0L;
        metricsToStore.clear();

        while (metric != null) {
          metricsToStore.add(metric);

          if (polledCount >= UNLOAD_COUNT) {
            break;
          }

          metric = metricQueue.poll();
          polledCount++;
        }

        metricStore.store(metricsToStore);
      }
    }

    /**
     * Wait metric from queue.
     *
     * @return Metric.
     */
    private Metric waitAvailableMetric() {
      try {
        return metricQueue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException("Metric unloader was interrupted");
      }
    }
  }
}
