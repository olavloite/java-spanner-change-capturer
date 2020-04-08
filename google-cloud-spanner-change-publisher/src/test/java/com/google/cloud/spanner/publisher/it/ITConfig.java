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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

/** Helper class for getting configuration values for integration tests. */
public class ITConfig extends com.google.cloud.spanner.poller.it.ITConfig {
  private static final Random RND = new Random();
  public static final String PUBSUB_PROJECT_ID =
      System.getProperty("pubsub.project", ServiceOptions.getDefaultProjectId());
  public static final String PUBSUB_CREDENTIALS_FILE = System.getProperty("pubsub.credentials");
  public static final String PUBSUB_TOPIC_ID =
      System.getProperty(
          "pubsub.topic", String.format("cdc-test-topic-%08d", RND.nextInt(100000000)));
  public static final String PUBSUB_SUBSCRIPTION_ID =
      System.getProperty(
          "pubsub.subscription",
          String.format("cdc-test-subscription-%08d", RND.nextInt(100000000)));

  public static Credentials getPubSubCredentials() throws IOException {
    if (PUBSUB_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(PUBSUB_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }
}
