/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.poller;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.poller.SpannerTableChangeCapturer.Row;
import com.google.cloud.spanner.poller.SpannerTableChangeCapturer.RowChangeCallback;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerTableTailerTest extends AbstractMockServerTest {
  @Test
  public void testReceiveChanges() throws Exception {
    DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(client, "Foo")
            .setPollInterval(Duration.ofSeconds(100L))
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT);
    tailer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(String table, Row row, Timestamp commitTimestamp) {
            receivedRows.incrementAndGet();
            latch.countDown();
          }
        });
    latch.await(5L, TimeUnit.SECONDS);
    tailer.stopAsync().get();
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT);
  }

  @Test
  public void testStressReceiveMultipleChanges() throws Exception {
    for (int i = 0; i < 1000; i++) {
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));
      SpannerTableTailer tailer =
          SpannerTableTailer.newBuilder(client, "Foo")
              .setPollInterval(Duration.ofMillis(1L))
              .build();
      final AtomicInteger receivedRows = new AtomicInteger();
      final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT);
      final int secondPollRowCount = 5;
      final CountDownLatch secondLatch =
          new CountDownLatch(SELECT_FOO_ROW_COUNT + secondPollRowCount);
      tailer.start(
          new RowChangeCallback() {
            @Override
            public void rowChange(String table, Row row, Timestamp commitTimestamp) {
              receivedRows.incrementAndGet();
              latch.countDown();
              secondLatch.countDown();
            }
          });
      latch.await(5L, TimeUnit.SECONDS);
      assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT);

      Timestamp lastSeenCommitTimestamp = tailer.getLastSeenCommitTimestamp();
      Timestamp nextCommitTimestamp =
          Timestamp.ofTimeSecondsAndNanos(
              lastSeenCommitTimestamp.getSeconds() + 1, lastSeenCommitTimestamp.getNanos());
      Statement pollStatement1 =
          SELECT_FOO_STATEMENT
              .toBuilder()
              .bind("prevCommitTimestamp")
              .to(lastSeenCommitTimestamp)
              .build();
      Statement pollStatement2 =
          SELECT_FOO_STATEMENT
              .toBuilder()
              .bind("prevCommitTimestamp")
              .to(nextCommitTimestamp)
              .build();
      mockSpanner.putStatementResults(
          StatementResult.query(
              pollStatement1,
              new RandomResultSetGenerator(5)
                  .generateWithFixedCommitTimestamp(nextCommitTimestamp)),
          StatementResult.query(pollStatement2, new RandomResultSetGenerator(0).generate()));

      secondLatch.await(5L, TimeUnit.SECONDS);
      assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT + secondPollRowCount);

      System.out.println("Finished test run " + i + ", closing tailer");
      tailer.stopAsync().get();
      stopServer();
      startStaticServer();
      System.out.println("Finished closing everything after test run " + i);
    }
  }
}
