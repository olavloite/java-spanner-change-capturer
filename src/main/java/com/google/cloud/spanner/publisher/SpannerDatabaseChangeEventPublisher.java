package com.google.cloud.spanner.publisher;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.capturer.SpannerDatabaseChangeCapturer;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.Builder;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Publishes change events from a Spanner database to PubSub topics.
 *
 * <p>The changes to the tables are emitted from a {@link SpannerDatabaseChangeCapturer} and then
 * sent to Pub/Sub by this event publisher.
 */
public class SpannerDatabaseChangeEventPublisher {
  private static final Logger logger =
      Logger.getLogger(SpannerDatabaseChangeEventPublisher.class.getName());

  public static class Builder {
    private final SpannerDatabaseChangeCapturer capturer;
    private String topicNameFormat;
    private Credentials credentials;

    private Builder(SpannerDatabaseChangeCapturer capturer) {
      this.capturer = capturer;
    }

    /**
     * Sets the format of the names of the topics where the events should be published. The name
     * format should be in the form 'projects/<project-id>/topics/<topic-id>', where <topic-id> may
     * contain the string %table%, which will be replaced with the actual table name.
     */
    public Builder setTopicNameFormat(String topicNameFormat) {
      this.topicNameFormat = Preconditions.checkNotNull(topicNameFormat);
      return this;
    }

    /**
     * Sets the credentials to use to publish to Pub/Sub. If no credentials are set, the credentials
     * returned by {@link GoogleCredentials#getApplicationDefault()} will be used.
     */
    public Builder setCredentials(Credentials credentials) {
      this.credentials = Preconditions.checkNotNull(credentials);
      return this;
    }

    public SpannerDatabaseChangeEventPublisher build() throws IOException {
      return new SpannerDatabaseChangeEventPublisher(this);
    }
  }

  public static Builder newBuilder(SpannerDatabaseChangeCapturer capturer) {
    return new Builder(capturer);
  }

  private boolean started;
  private boolean stopped;
  private final SpannerDatabaseChangeCapturer capturer;
  private final List<SpannerTableChangeEventPublisher> publishers;

  private SpannerDatabaseChangeEventPublisher(Builder builder) throws IOException {
    this.capturer = builder.capturer;
    this.publishers = new ArrayList<>(capturer.getTables().size());
    for (String table : capturer.getTables()) {
      publishers.add(
          SpannerTableChangeEventPublisher.newBuilder(capturer.getCapturer(table))
              .setCredentials(builder.credentials)
              .setTopicName(builder.topicNameFormat.replace("%table%", table))
              .build());
    }
  }

  public void start() {
    Preconditions.checkArgument(!started, "This event publisher has already been started");
    started = true;
    for (SpannerTableChangeEventPublisher publisher : publishers) {
      publisher.start();
    }
  }

  public void stop() {
    Preconditions.checkArgument(started, "This event publisher has not been started");
    Preconditions.checkArgument(!stopped, "This event publisher has already been stopped");
    stopped = true;
    for (SpannerTableChangeEventPublisher publisher : publishers) {
      publisher.stop();
    }
  }
}
