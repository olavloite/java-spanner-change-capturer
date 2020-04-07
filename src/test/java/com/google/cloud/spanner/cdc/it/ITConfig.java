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

import com.google.api.services.cloudfunctions.v1.CloudFunctionsScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

/** Helper class for getting configuration values for integration tests. */
class ITConfig {
  private static final Random RND = new Random();
  static final String SPANNER_PROJECT_ID =
      System.getProperty("spanner.project", ServiceOptions.getDefaultProjectId());
  static final String SPANNER_CREDENTIALS_FILE = System.getProperty("spanner.credentials");
  static final String SPANNER_INSTANCE_ID = System.getProperty("spanner.instance", "test-instance");
  static final String DATABASE_ID = String.format("cdc-test-db-%08d", RND.nextInt(100000000));

  static final String PUBSUB_PROJECT_ID =
      System.getProperty("pubsub.project", ServiceOptions.getDefaultProjectId());
  static final String PUBSUB_CREDENTIALS_FILE = System.getProperty("pubsub.credentials");
  static final String PUBSUB_TOPIC_ID =
      System.getProperty(
          "pubsub.topic", String.format("cdc-test-topic-%08d", RND.nextInt(100000000)));
  static final String PUBSUB_SUBSCRIPTION_ID =
      System.getProperty(
          "pubsub.subscription",
          String.format("cdc-test-subscription-%08d", RND.nextInt(100000000)));

  static final String FUNCTIONS_PROJECT_ID =
      System.getProperty("functions.project", ServiceOptions.getDefaultProjectId());
  static final String FUNCTIONS_CREDENTIALS_FILE = System.getProperty("functions.credentials");
  static final String FUNCTIONS_FUNCTION_ID =
      System.getProperty(
          "functions.function", String.format("cdc-test-function-%08d", RND.nextInt(100000000)));
  static final String FUNCTIONS_SERVICE_ACCOUNT_EMAIL =
      System.getProperty("functions.serviceAccountEmail");

  static final String STORAGE_PROJECT_ID =
      System.getProperty("storage.project", ServiceOptions.getDefaultProjectId());
  static final String STORAGE_CREDENTIALS_FILE = System.getProperty("storage.credentials");
  static final String STORAGE_BUCKET_NAME =
      System.getProperty(
          "storage.bucket", String.format("cdc-test-changes-%08d", RND.nextInt(100000000)));

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

  static Credentials getCloudFunctionsCredentials() throws IOException {
    if (FUNCTIONS_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(FUNCTIONS_CREDENTIALS_FILE))
          .createScoped(CloudFunctionsScopes.all());
    }
    return GoogleCredentials.getApplicationDefault().createScoped(CloudFunctionsScopes.all());
  }

  static Credentials getStorageCredentials() throws IOException {
    if (STORAGE_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(STORAGE_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }
}
