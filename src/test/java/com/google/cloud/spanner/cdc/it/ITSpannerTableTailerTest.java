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

package com.google.cloud.spanner.cdc.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.cdc.SpannerTableChangeCapturer.Row;
import com.google.cloud.spanner.cdc.SpannerTableChangeCapturer.RowChangeCallback;
import com.google.cloud.spanner.cdc.SpannerTableTailer;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class ITSpannerTableTailerTest {
  private static final Logger logger = Logger.getLogger(ITSpannerTableTailerTest.class.getName());
  private static final String DATABASE_ID =
      String.format("cdc-db-%08d", new Random().nextInt(100000000));
  private static Spanner spanner;
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    spanner =
        SpannerOptions.newBuilder()
            .setProjectId(ITConfig.SPANNER_PROJECT_ID)
            .setCredentials(ITConfig.getSpannerCredentials())
            .build()
            .getService();
    database =
        spanner
            .getDatabaseAdminClient()
            .createDatabase(
                ITConfig.SPANNER_INSTANCE_ID,
                DATABASE_ID,
                Arrays.asList(
                    "CREATE TABLE NUMBERS (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                    "CREATE TABLE LAST_SEEN_COMMIT_TIMESTAMPS (TABLE_NAME STRING(MAX) NOT NULL, LAST_SEEN_COMMIT_TIMESTAMP TIMESTAMP NOT NULL) PRIMARY KEY (TABLE_NAME)"))
            .get();
    logger.info(String.format("Created database %s", DATABASE_ID.toString()));
  }

  @AfterClass
  public static void teardown() {
    database.drop();
    spanner.close();
  }

  @Test
  public void testSpannerTailer() throws InterruptedException {
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(client, "NUMBERS")
            .setPollInterval(Duration.ofMillis(10L))
            .build();
    final Queue<Struct> changes = new ConcurrentLinkedQueue<>();
    tailer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(String table, Row row, Timestamp commitTimestamp) {
            logger.info(
                String.format(
                    "Received changed for table %s: %s", table, row.asStruct().toString()));
            changes.add(row.asStruct());
          }
        });
    Timestamp commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("ONE")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("TWO")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(3L)
                    .set("NAME")
                    .to("THREE")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build()));
    List<Struct> inserts = retrieveChanges(changes, 3);
    assertThat(inserts).hasSize(3);
    assertThat(inserts)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("ONE")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build()));
    inserts = retrieveChanges(changes, 2);
    assertThat(inserts).hasSize(2);
    assertThat(inserts)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("FOUR")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("FIVE")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("five")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build()));
    List<Struct> updates = retrieveChanges(changes, 2);
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("one")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("five")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    // Verify that deletes are not picked up by the poller.
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.delete("NUMBERS", Key.of(2L)), Mutation.delete("NUMBERS", Key.of(3L))));
    Thread.sleep(500L);
    assertThat(changes).isEmpty();
  }

  private List<Struct> retrieveChanges(Queue<Struct> changes, int number) {
    Stopwatch watch = Stopwatch.createStarted();
    List<Struct> res = new ArrayList<>(number);
    while (res.size() < number && watch.elapsed(TimeUnit.SECONDS) < 5L) {
      if (changes.peek() != null) {
        res.add(changes.remove());
      }
    }
    return res;
  }
}
