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

package com.google.cloud.spanner.publisher.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.poller.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.poller.SpannerDatabaseChangeCapturer;
import com.google.cloud.spanner.poller.SpannerDatabaseTailer;
import com.google.cloud.spanner.publisher.SpannerDatabaseChangeEventPublisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class ITSpannerDatabaseChangeEventPublisherTest {
  private static final Logger logger =
      Logger.getLogger(ITSpannerDatabaseChangeEventPublisherTest.class.getName());
  private static final String[] tables = new String[] {"NUMBERS1", "NUMBERS2"};
  private static Spanner spanner;
  private static TopicAdminClient topicAdminClient;
  private static SubscriptionAdminClient subAdminClient;
  private static Database database;
  private static Subscriber[] subscribers;
  private static List<List<PubsubMessage>> receivedMessages;
  private static CountDownLatch receivedMessagesCount = new CountDownLatch(0);

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
                ITConfig.DATABASE_ID,
                Arrays.asList(
                    "CREATE TABLE NUMBERS1 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                    "CREATE TABLE NUMBERS2 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)"))
            .get();
    logger.info(String.format("Created database %s", ITConfig.DATABASE_ID.toString()));

    topicAdminClient =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(
                    FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
                .build());
    subAdminClient =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(
                    FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
                .build());

    receivedMessages = new ArrayList<>(tables.length);
    subscribers = new Subscriber[tables.length];
    int i = 0;
    for (String table : tables) {
      receivedMessages.add(Collections.synchronizedList(new ArrayList<PubsubMessage>()));

      topicAdminClient.createTopic(
          String.format("projects/%s/topics/spanner-update-%s", ITConfig.PUBSUB_PROJECT_ID, table));
      logger.info(String.format("Created topic for table %s", table));

      subAdminClient.createSubscription(
          String.format(
              "projects/%s/subscriptions/spanner-update-%s", ITConfig.PUBSUB_PROJECT_ID, table),
          String.format("projects/%s/topics/spanner-update-%s", ITConfig.PUBSUB_PROJECT_ID, table),
          PushConfig.getDefaultInstance(),
          10);
      logger.info(String.format("Created subscription spanner-update-%s", table));

      final int index = i;
      subscribers[i] =
          Subscriber.newBuilder(
                  String.format(
                      "projects/%s/subscriptions/spanner-update-%s",
                      ITConfig.PUBSUB_PROJECT_ID, table),
                  new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                      logger.info(String.format("Received message %s", message.toString()));
                      receivedMessages.get(index).add(message);
                      receivedMessagesCount.countDown();
                      consumer.ack();
                    }
                  })
              .setCredentialsProvider(
                  FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
              .build();
      subscribers[i].startAsync().awaitRunning();
      i++;
    }
  }

  @AfterClass
  public static void teardown() {
    database.drop();
    logger.info("Dropped test database");
    spanner.close();
    for (Subscriber subscriber : subscribers) {
      subscriber.stopAsync();
    }

    for (String table : tables) {
      subAdminClient.deleteSubscription(
          String.format(
              "projects/%s/subscriptions/spanner-update-%s", ITConfig.PUBSUB_PROJECT_ID, table));
      logger.info(String.format("Dropped test subscription %s", table));
      topicAdminClient.deleteTopic(
          String.format("projects/%s/topics/spanner-update-%s", ITConfig.PUBSUB_PROJECT_ID, table));
      logger.info(String.format("Dropped test topic %s", table));
    }
  }

  @Test
  public void testEventPublisher() throws Exception {
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    SpannerDatabaseChangeCapturer capturer =
        SpannerDatabaseTailer.newBuilder(spanner, database.getId())
            .setAllTables()
            .setPollInterval(Duration.ofMillis(50L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .setCreateTableIfNotExists()
                    .build())
            .build();
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(capturer)
            .setTopicNameFormat(
                String.format(
                    "projects/%s/topics/spanner-update-%%table%%", ITConfig.PUBSUB_PROJECT_ID))
            .setCredentials(ITConfig.getPubSubCredentials())
            .build();
    eventPublisher.start();

    receivedMessagesCount = new CountDownLatch(4);
    client.writeAtLeastOnce(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("NUMBERS1")
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("ONE")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS2")
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS1")
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS2")
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("FOUR")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build()));
    receivedMessagesCount.await(10L, TimeUnit.SECONDS);
    assertThat(receivedMessages.get(0)).hasSize(2);
    assertThat(receivedMessages.get(1)).hasSize(2);
    eventPublisher.stop();
    assertThat(eventPublisher.awaitTermination(10L, TimeUnit.SECONDS)).isTrue();
  }
}
