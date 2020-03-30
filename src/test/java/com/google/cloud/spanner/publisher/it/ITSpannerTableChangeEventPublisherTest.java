package com.google.cloud.spanner.publisher.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.FixedCredentialsProvider;
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
import com.google.cloud.spanner.capturer.SpannerTableChangeCapturer;
import com.google.cloud.spanner.capturer.SpannerTableTailer;
import com.google.cloud.spanner.capturer.it.ITConfig;
import com.google.cloud.spanner.capturer.it.ITSpannerTableTailerTest;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
public class ITSpannerTableChangeEventPublisherTest {
  private static final Logger logger = Logger.getLogger(ITSpannerTableTailerTest.class.getName());
  private static final String DATABASE_ID =
      String.format("cdc-db-%08d", new Random().nextInt(100000000));
  private static Spanner spanner;
  private static TopicAdminClient topicAdminClient;
  private static SubscriptionAdminClient subAdminClient;
  private static Database database;
  private static Subscriber subscriber;
  private static List<PubsubMessage> receivedMessages =
      Collections.synchronizedList(new ArrayList<>());
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
                DATABASE_ID,
                Arrays.asList(
                    "CREATE TABLE NUMBERS (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                    "CREATE TABLE LAST_SEEN_COMMIT_TIMESTAMPS (TABLE_NAME STRING(MAX) NOT NULL, LAST_SEEN_COMMIT_TIMESTAMP TIMESTAMP NOT NULL) PRIMARY KEY (TABLE_NAME)"))
            .get();
    logger.info(String.format("Created database %s", DATABASE_ID.toString()));

    topicAdminClient =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(
                    FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
                .build());
    topicAdminClient.createTopic(
        String.format(
            "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID));
    logger.info(String.format("Created topic %s", ITConfig.PUBSUB_TOPIC_ID));

    subAdminClient =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(
                    FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
                .build());
    subAdminClient.createSubscription(
        String.format(
            "projects/%s/subscriptions/%s",
            ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_SUBSCRIPTION_ID),
        String.format(
            "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID),
        PushConfig.getDefaultInstance(),
        10);
    logger.info(String.format("Created subscription %s", ITConfig.PUBSUB_SUBSCRIPTION_ID));

    subscriber =
        Subscriber.newBuilder(
                String.format(
                    "projects/%s/subscriptions/%s",
                    ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_SUBSCRIPTION_ID),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    logger.info(String.format("Received message %s", message.toString()));
                    receivedMessages.add(message);
                    receivedMessagesCount.countDown();
                    consumer.ack();
                  }
                })
            .setCredentialsProvider(
                FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
            .build();
    subscriber.startAsync().awaitRunning();
  }

  @AfterClass
  public static void teardown() {
    database.drop();
    logger.info("Dropped test database");
    spanner.close();
    subscriber.stopAsync();

    subAdminClient.deleteSubscription(
        String.format(
            "projects/%s/subscriptions/%s",
            ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_SUBSCRIPTION_ID));
    logger.info("Dropped test subscription");
    topicAdminClient.deleteTopic(
        String.format(
            "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID));
    logger.info("Dropped test topic");
  }

  @Test
  public void testEventPublisher() throws Exception {
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    SpannerTableChangeCapturer capturer =
        SpannerTableTailer.newBuilder(client, "NUMBERS")
            .setPollInterval(Duration.ofMillis(50L))
            .build();
    SpannerTableChangeEventPublisher eventPublisher =
        SpannerTableChangeEventPublisher.newBuilder(capturer)
            .setTopicName(
                String.format(
                    "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID))
            .setCredentials(ITConfig.getPubSubCredentials())
            .build();
    eventPublisher.start();

    receivedMessagesCount = new CountDownLatch(3);
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
    receivedMessagesCount.await(10L, TimeUnit.SECONDS);
    assertThat(receivedMessages).hasSize(3);
    eventPublisher.stop();
  }
}
