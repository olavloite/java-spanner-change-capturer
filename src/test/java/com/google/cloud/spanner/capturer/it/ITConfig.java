package com.google.cloud.spanner.capturer.it;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import java.io.FileInputStream;
import java.io.IOException;

public class ITConfig {
  public static final String SPANNER_PROJECT_ID =
      System.getProperty("spanner.project", ServiceOptions.getDefaultProjectId());
  public static final String SPANNER_CREDENTIALS_FILE = System.getProperty("spanner.credentials");
  public static final String SPANNER_INSTANCE_ID =
      System.getProperty("spanner.instance", "test-instance");
  public static final String PUBSUB_PROJECT_ID =
      System.getProperty("pubsub.project", ServiceOptions.getDefaultProjectId());
  public static final String PUBSUB_CREDENTIALS_FILE = System.getProperty("pubsub.credentials");
  public static final String PUBSUB_TOPIC_ID =
      System.getProperty("pubsub.topic", "NUMBERS-updates");
  public static final String PUBSUB_SUBSCRIPTION_ID =
      System.getProperty("pubsub.subscription", "NUMBERS-updates");

  public static Credentials getSpannerCredentials() throws IOException {
    if (SPANNER_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(SPANNER_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }

  public static Credentials getPubSubCredentials() throws IOException {
    if (PUBSUB_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(PUBSUB_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }
}
