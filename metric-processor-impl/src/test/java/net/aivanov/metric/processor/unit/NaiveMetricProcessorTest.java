package net.aivanov.metric.processor.unit;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.times;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import net.aivanov.metric.Metric;
import net.aivanov.metric.processor.MetricProcessor;
import net.aivanov.metric.processor.NaiveMetricProcessor;
import net.aivanov.metric.store.MetricStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for naive metric processor.
 *
 * @author Artem_Ivanov
 */
@DisplayName("Testing naive metric processor")
public class NaiveMetricProcessorTest {

  private MetricStore metricStore;
  private MetricProcessor metricProcessor;

  @Captor
  private ArgumentCaptor<Collection<Metric>> metricsCaptor;

  @BeforeEach
  public void init() {
    MockitoAnnotations.initMocks(this);

    this.metricStore = Mockito.mock(MetricStore.class);
    this.metricProcessor = new NaiveMetricProcessor(metricStore);

    Mockito.reset(metricStore);
  }

  @Test
  public void testAddMetric() {
    Metric metric = new Metric('d', 1, 0);

    Mockito.doNothing().when(metricStore).store(anyCollection());

    metricProcessor.add(metric.getTimestamp(), metric.getKey(), metric.getValue());

    Mockito.verify(metricStore, times(1)).store(metricsCaptor.capture());

    Collection<Metric> metrics = metricsCaptor.getValue();

    Assertions.assertFalse(metrics.isEmpty());
    Assertions.assertEquals(metrics.size(), 1);
    Assertions.assertEquals(metrics.stream().findFirst().get(), metric);
  }

  @Test
  public void testSumMetric() {
    Metric metric = new Metric('d', 1, 0);

    Mockito.doReturn(1L).when(metricStore).sumAllValuesByKeyAndRange(metric.getKey(), 0, 1);

    long sum = metricProcessor.sum(0, 1, metric.getKey());

    Mockito.verify(metricStore, times(1)).sumAllValuesByKeyAndRange(metric.getKey(), 0, 1);

    Assertions.assertEquals(sum, 1);
  }
}
