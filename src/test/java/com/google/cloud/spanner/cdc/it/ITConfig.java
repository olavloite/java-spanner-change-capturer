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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import java.io.FileInputStream;
import java.io.IOException;

/** Helper class for getting configuration values for integration tests. */
class ITConfig {
  static final String SPANNER_PROJECT_ID =
      System.getProperty("spanner.project", ServiceOptions.getDefaultProjectId());
  static final String SPANNER_CREDENTIALS_FILE = System.getProperty("spanner.credentials");
  static final String SPANNER_INSTANCE_ID = System.getProperty("spanner.instance", "test-instance");
  static final String PUBSUB_PROJECT_ID =
      System.getProperty("pubsub.project", ServiceOptions.getDefaultProjectId());
  static final String PUBSUB_CREDENTIALS_FILE = System.getProperty("pubsub.credentials");
  static final String PUBSUB_TOPIC_ID = System.getProperty("pubsub.topic", "NUMBERS-updates");
  static final String PUBSUB_SUBSCRIPTION_ID =
      System.getProperty("pubsub.subscription", "NUMBERS-updates");

  static Credentials getSpannerCredentials() throws IOException {
    if (SPANNER_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(SPANNER_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }

  static Credentials getPubSubCredentials() throws IOException {
    if (PUBSUB_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(PUBSUB_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }
}
