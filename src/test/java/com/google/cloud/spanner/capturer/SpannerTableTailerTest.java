package com.google.cloud.spanner.capturer;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.capturer.SpannerTableChangeCapturer.Row;
import com.google.cloud.spanner.capturer.SpannerTableChangeCapturer.RowChangeCallback;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerTableTailerTest extends AbstractMockServerTest {
  @Test
  public void testCallback() throws Exception {
    DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(client, "Foo")
            .setPollInterval(Duration.ofSeconds(100L))
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    tailer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(String table, Row row) {
            receivedRows.incrementAndGet();
          }
        });
    Thread.sleep(500L);
    tailer.stop();
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT);
  }
}
