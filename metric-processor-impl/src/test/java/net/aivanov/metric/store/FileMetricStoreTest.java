package net.aivanov.metric.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import net.aivanov.metric.Metric;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for file metric store.
 *
 * @author Artem_Ivanov
 */
@DisplayName("Test file metric store")
public class FileMetricStoreTest {

  /**
   * Test store path.
   */
  private final static String TEST_STORE_PATH = "src/test/resources/store";

  /**
   * Test prepared files path.
   */
  private final static String TEST_PREPARED_FILES_PATH = "src/test/resources/test_files";

  /**
   * Metric store for testing.
   */
  private final MetricStore metricStoreWrite = new FileMetricStore(TEST_STORE_PATH);

  /**
   * Metric store for reading tests.
   */
  private final MetricStore metricStoreRead = new FileMetricStore(TEST_PREPARED_FILES_PATH);

  @BeforeEach
  public void cleanUp() {
    clearFileStore();
  }

  /**
   * Delete test files.
   */
  private void clearFileStore() {
    try {
      Files.walk(Paths.get(TEST_STORE_PATH))
          .filter(p -> Files.isRegularFile(p))
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      throw new RuntimeException("Can't delete test store");
    }
  }

  @Test
  void testStoreEmptyCollection() {
    metricStoreWrite.store(Collections.emptyList());
  }

  @Test
  void testStoreNonEmptyCollection() {
    Collection<Metric> metrics = Arrays.asList(
        new Metric('a', 1, 0),
        new Metric('b', 1, 1),
        new Metric('a', -3, 2)
    );

    metricStoreWrite.store(metrics);

    List<Path> files = getFilesFromPath(TEST_STORE_PATH);

    Assertions.assertEquals(files.size(), 1);
    Assertions.assertTrue(readFileLines(files.get(0)).containsAll(Arrays.asList(
        "a;1;0",
        "b;1;1",
        "a;-3;2"
    )));
  }

  @Test
  void testSumAllValuesByKeyInRange() {
    long sum = metricStoreRead.sumAllValuesByKeyAndRange('a', 0, 3);

    Assertions.assertEquals(sum, -2);
  }

  @Test
  void testSumAllValuesByKeyOutOfRange() {
    long sum = metricStoreRead.sumAllValuesByKeyAndRange('a', 4, 5);

    Assertions.assertEquals(sum, 0);
  }

  @Test
  void testSumAllValuesByKeyExcludingEndOfRange() {
    long sum = metricStoreRead.sumAllValuesByKeyAndRange('a', 0, 2);

    Assertions.assertEquals(sum, 1);
  }

  /**
   * Read files from path.
   *
   * @param path Path.
   * @return List of files.
   */
  private static List<Path> getFilesFromPath(String path) {
    try {
      return Files.walk(Paths.get(path))
          .filter(e -> Files.isRegularFile(e))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  /**
   * Read file lines or throw runtime exception.
   *
   * @param filePath File path.
   * @return List of string.
   */
  private static List<String> readFileLines(Path filePath) {
    try {
      return Files.readAllLines(filePath);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }
}
