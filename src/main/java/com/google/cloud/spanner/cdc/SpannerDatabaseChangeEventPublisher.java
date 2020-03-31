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

package com.google.cloud.spanner.cdc;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
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
    logger.log(Level.FINE, "Starting event publisher");
    started = true;
    for (SpannerTableChangeEventPublisher publisher : publishers) {
      publisher.start();
    }
  }

  public void stop() {
    Preconditions.checkArgument(started, "This event publisher has not been started");
    Preconditions.checkArgument(!stopped, "This event publisher has already been stopped");
    logger.log(Level.FINE, "Stopping event publisher");
    stopped = true;
    for (SpannerTableChangeEventPublisher publisher : publishers) {
      publisher.stop();
    }
  }

  public boolean awaitTermination(long duration, TimeUnit unit) throws InterruptedException {
    Stopwatch watch = Stopwatch.createStarted();
    boolean res = true;
    for (SpannerTableChangeEventPublisher publisher : publishers) {
      long remainingDuration = duration - watch.elapsed(unit);
      if (remainingDuration <= 0) {
        return false;
      }
      res = res && publisher.awaitTermination(duration, unit);
    }
    return res;
  }
}
