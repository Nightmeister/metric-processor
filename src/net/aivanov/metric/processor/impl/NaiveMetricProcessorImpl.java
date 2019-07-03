package net.aivanov.metric.processor.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.aivanov.metric.processor.MetricProcessor;

/**
 * Naive metric processor implementation.
 *
 * @author Artem_Ivanov
 */
public class NaiveMetricProcessorImpl implements MetricProcessor {

  /**
   * Metric queue capacity.
   */
  private final static int METRIC_QUEUE_CAPACITY = 10000000;

  /**
   * Metric queue comparator
   */
  private final static Comparator<Metric> METRIC_QUEUE_COMPARATOR =
      Comparator.<Metric>comparingLong(metric -> metric.timestamp);

  /**
   * Metric queue.
   */
  private final BlockingQueue<Metric> metrics =
      new PriorityBlockingQueue<>(METRIC_QUEUE_CAPACITY, METRIC_QUEUE_COMPARATOR);

  /**
   * Metric store.
   */
  private final FileMetricStore metricStore;

  /**
   * Metric queue unloader.
   */
  private final MetricQueueStoreUnloader metricQueueStoreUnloader;

  public NaiveMetricProcessorImpl(String storeAbsoluteFilePath) {
    this.metricStore = new FileMetricStore(storeAbsoluteFilePath);
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
   * File store for metrics.
   */
  static class FileMetricStore {

    /**
     * File name pattern.
     */
    private final static Pattern FILE_NAME_PATTERN = Pattern.compile("^(\\d+)[_](\\d+)[-](\\S+)$");

    /**
     * Storage files path.
     */
    private final String storeAbsoluteFilePath;

    FileMetricStore(String storeAbsoluteFilePath) {
      this.storeAbsoluteFilePath = storeAbsoluteFilePath;
    }

    void store(Collection<Metric> metrics) {
      if (!metrics.isEmpty()) {
        List<Metric> metricListToWrite = new ArrayList<>(metrics);
        String fileName = generateFileNameForMetricList(metricListToWrite);
        writeFile(Path.of(storeAbsoluteFilePath, fileName), createMetricFileContent(metricListToWrite));
      }
    }

    long sumAllValuesByKeyAndRange(char key, long startTimestampInclusive, long endTimestampExclusive) {
      try (Stream<Path> walk = Files.walk(Paths.get(storeAbsoluteFilePath))) {
        return walk
            .filter(Files::isRegularFile)
            .filter(file -> isFileInRange(file, startTimestampInclusive, endTimestampExclusive))
            .map(FileMetricStore::readMetricsFromFile)
            .flatMap(Collection::stream)
            .filter(e -> e.key == key && (e.timestamp >= startTimestampInclusive && e.timestamp < endTimestampExclusive))
            .reduce(0L, (a, v) -> a + v.value, Long::sum);
      } catch (IOException e) {
        throw new RuntimeException("Error while getting metrics", e);
      }
    }

    /**
     * Match file timestamp range.
     *
     * @param file File.
     * @param startTimestampInclusive Start timestamp.
     * @param endTimestampExclusive End timestamp.
     * @return Result.
     */
    private static boolean isFileInRange(Path file, long startTimestampInclusive, long endTimestampExclusive) {
      String fileName = file.getFileName().toString();
      Matcher fileNameMatcher = FILE_NAME_PATTERN.matcher(fileName);

      if (fileNameMatcher.find()) {
        long startTimestamp = Long.parseLong(fileNameMatcher.group(1));
        long endTimestamp = Long.parseLong(fileNameMatcher.group(2));

        return isRangesIntersected(startTimestamp, endTimestamp, startTimestampInclusive, endTimestampExclusive);
      } else {
        return false;
      }
    }

    /**
     * Is ranged intersected?
     *
     * @param start1 Start range 1.
     * @param end1 End range 1.
     * @param start2 Start range 2.
     * @param end2 End range 2.
     * @return Result.
     */
    private static boolean isRangesIntersected(long start1, long end1, long start2, long end2) {
      return start1 <= end2 && end1 >= start2;
    }

    /**
     * Generate unique filename.
     *
     * @param metrics List of metrics.
     * @return Unique file name.
     */
    private static String generateFileNameForMetricList(List<Metric> metrics) {
      long startTimestamp = metrics.get(0).timestamp;
      long endTimestamp = metrics.get(metrics.size() - 1).timestamp;

      return String.format("%d_%d-%s", startTimestamp, endTimestamp, UUID.randomUUID());
    }

    /**
     * Create metric file content.
     *
     * @param metrics List of metric.
     * @return File content as bytes.
     */
    private static byte[] createMetricFileContent(List<Metric> metrics) {
      return metrics.stream()
          .map(FileMetricStore::convertMetricToString)
          .collect(Collectors.joining("\n"))
          .getBytes();
    }

    /**
     * Convert metric to string.
     *
     * @param metric Metric.
     * @return String.
     */
    private static String convertMetricToString(Metric metric) {
      return String.format("%c;%d;%d", metric.key, metric.value, metric.timestamp);
    }

    /**
     * Convert string to metric.
     *
     * @param str String.
     * @return Metric.
     */
    private static Metric convertStringToMetric(String str) {
      String[] splittedString = str.split(";");

      if (splittedString.length != 3) {
        throw new RuntimeException("Wrong metric string value");
      }

      return new Metric(
          splittedString[0].charAt(0),
          Integer.parseInt(splittedString[1]),
          Long.parseLong(splittedString[2])
      );
    }

    /**
     * Write file.
     *
     * @param path File path.
     * @param content File content.
     */
    private static void writeFile(Path path, byte[] content) {
      try {
        Files.write(path, content);
      } catch (IOException e) {
        throw new RuntimeException("Error file write", e);
      }
    }

    /**
     * Read metrics from file.
     *
     * @param path Path.
     * @return List of metrics.
     */
    private static List<Metric> readMetricsFromFile(Path path) {
      try {
        return Files.readAllLines(path).stream()
            .map(FileMetricStore::convertStringToMetric)
            .collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException("Error file read", e);
      }
    }
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
    private final FileMetricStore metricStore;

    /**
     * Metric queue.
     */
    private final BlockingQueue<Metric> metricQueue;

    /**
     * Unload executor.
     */
    private final ExecutorService unloadExecutor = Executors.newFixedThreadPool(1);

    MetricQueueStoreUnloader(BlockingQueue<Metric> metricQueue, FileMetricStore metricStore) {
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

  /**
   * Metric data class.
   */
  static class Metric {

    /**
     * Key.
     */
    final char key;

    /**
     * Value.
     */
    final int value;

    /**
     * Timestamp.
     */
    final long timestamp;

    Metric(char key, int value, long timestamp) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
    }
  }
}
