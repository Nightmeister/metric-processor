package net.aivanov.metric.store;

/**
 * @author Artem_Ivanov
 */

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.aivanov.metric.Metric;

/**
 * File store for metrics.
 *
 * @author Artem_Ivanov.
 */
public class FileMetricStore implements MetricStore {

  /**
   * File name pattern.
   */
  private final static Pattern FILE_NAME_PATTERN = Pattern.compile("^(\\d+)[_](\\d+)[-](\\S+)$");

  /**
   * Storage store path.
   */
  private final String storeAbsoluteFilePath;

  public FileMetricStore(String storeAbsoluteFilePath) {
    this.storeAbsoluteFilePath = storeAbsoluteFilePath;
  }

  @Override
  public void store(Collection<Metric> metrics) {
    if (!metrics.isEmpty()) {
      List<Metric> metricListToWrite = new ArrayList<>(metrics);
      String fileName = generateFileNameForMetricList(metricListToWrite);
      writeFile(Path.of(storeAbsoluteFilePath, fileName), createMetricFileContent(metricListToWrite));
    }
  }

  @Override
  public long sumAllValuesByKeyAndRange(char key, long startTimestampInclusive, long endTimestampExclusive) {
    try (Stream<Path> walk = Files.walk(Paths.get(storeAbsoluteFilePath))) {
      return walk
          .filter(Files::isRegularFile)
          .filter(file -> isFileInRange(file, startTimestampInclusive, endTimestampExclusive))
          .map(FileMetricStore::readMetricsFromFile)
          .flatMap(Collection::stream)
          .filter(e -> e.getKey() == key && (e.getTimestamp() >= startTimestampInclusive && e.getTimestamp() < endTimestampExclusive))
          .reduce(0L, (a, v) -> a + v.getValue(), Long::sum);
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
    long startTimestamp = metrics.get(0).getTimestamp();
    long endTimestamp = metrics.get(metrics.size() - 1).getTimestamp();

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
    return String.format("%c;%d;%d", metric.getKey(), metric.getValue(), metric.getTimestamp());
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
