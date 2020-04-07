package com.google.cloud.spanner.cdc;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.cdc.MockPubSubServer.MockPublisherServiceImpl;
import com.google.cloud.spanner.cdc.MockPubSubServer.MockSubscriberServiceImpl;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerDatabaseChangeEventPublisherTest extends AbstractMockServerTest {
  private static MockPubSubServer mockPubSub;
  private static Server pubSubServer;
  private static InetSocketAddress pubSubAddress;
  private static MockPublisherServiceImpl publisherService;
  private static MockSubscriberServiceImpl subscriberService;

  @BeforeClass
  public static void setupPubSub() throws IOException {
    mockPubSub = new MockPubSubServer();
    publisherService = mockPubSub.new MockPublisherServiceImpl();
    subscriberService = mockPubSub.new MockSubscriberServiceImpl();
    pubSubAddress = new InetSocketAddress("localhost", 0);
    pubSubServer =
        NettyServerBuilder.forAddress(pubSubAddress)
            .addService(publisherService)
            .addService(subscriberService)
            .build()
            .start();
  }

  @AfterClass
  public static void teardownPubSub() throws InterruptedException {
    pubSubServer.shutdown();
    pubSubServer.awaitTermination();
  }

  @Before
  public void setup() {
    publisherService.reset();
    subscriberService.reset();
  }

  @Test
  public void testPublishChanges() throws Exception {
    DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));

    final AtomicInteger receivedMessages = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);

    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:" + pubSubServer.getPort())
            .usePlaintext()
            .build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
    Subscriber subscriber =
        Subscriber.newBuilder(
                "projects/p/subscriptions/s",
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    latch.countDown();
                    receivedMessages.incrementAndGet();
                    consumer.ack();
                  }
                })
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
    subscriber.startAsync().awaitRunning();

    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(client)
            .setAllTables()
            .setPollInterval(Duration.ofSeconds(100L))
            .build();
    SpannerDatabaseChangeEventPublisher publisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(tailer)
            .usePlainText()
            .setEndpoint("localhost:" + pubSubServer.getPort())
            .setTopicNameFormat("projects/p/topics/%table%-updates")
            .build();
    publisher.start();
    latch.await(10L, TimeUnit.SECONDS);
    publisher.stop();
    assertThat(receivedMessages.get()).isEqualTo(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
    publisher.awaitTermination(5L, TimeUnit.SECONDS);
    subscriber.stopAsync().awaitTerminated();
  }
}