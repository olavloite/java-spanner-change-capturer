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

package com.google.cloud.spanner.archiver.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.cloudfunctions.v1.CloudFunctions;
import com.google.api.services.cloudfunctions.v1.model.CloudFunction;
import com.google.api.services.cloudfunctions.v1.model.EventTrigger;
import com.google.api.services.cloudfunctions.v1.model.GenerateUploadUrlRequest;
import com.google.api.services.cloudfunctions.v1.model.GenerateUploadUrlResponse;
import com.google.api.services.cloudfunctions.v1.model.Operation;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.poller.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.poller.SpannerTableChangeCapturer;
import com.google.cloud.spanner.poller.SpannerTableTailer;
import com.google.cloud.spanner.poller.TableId;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.PublishListener;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.IamConfiguration;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Stopwatch;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class ITArchiverTest {
  private static final Logger logger = Logger.getLogger(ITArchiverTest.class.getName());
  private static Spanner spanner;
  private static Database database;
  private static TopicAdminClient topicAdminClient;
  private static Storage storage;
  private static String storageLocation = "us-east1";
  private static CloudFunctions functions;
  private static String functionLocation = "us-east1";

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
                Collections.singleton(
                    "CREATE TABLE NUMBERS (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)"))
            .get();
    logger.info(String.format("Created database %s", ITConfig.DATABASE_ID.toString()));

    topicAdminClient =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(
                    FixedCredentialsProvider.create(ITConfig.getPubSubCredentials()))
                .build());
    String topicName =
        String.format(
            "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID);
    topicAdminClient.createTopic(topicName);
    logger.info(String.format("Created topic %s", topicName));

    storage =
        StorageOptions.newBuilder()
            .setProjectId(ITConfig.STORAGE_PROJECT_ID)
            .setCredentials(ITConfig.getStorageCredentials())
            .build()
            .getService();
    storage.create(
        BucketInfo.newBuilder(ITConfig.STORAGE_BUCKET_NAME)
            .setLocation(storageLocation)
            .setStorageClass(StorageClass.ARCHIVE)
            .setIamConfiguration(
                IamConfiguration.newBuilder().setIsUniformBucketLevelAccessEnabled(true).build())
            .build());

    // Create cloud function.
    functions =
        new CloudFunctions(
            GoogleNetHttpTransport.newTrustedTransport(),
            JacksonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(ITConfig.getCloudFunctionsCredentials()));
    createArchiverFunction(
        ITConfig.FUNCTIONS_PROJECT_ID, functionLocation, ITConfig.FUNCTIONS_FUNCTION_ID, topicName);
  }

  private static void createArchiverFunction(
      String project, String location, String functionId, String topicName) throws Exception {
    // Upload the source code to a signed URL.
    String sourceUrl = uploadSourceCode();

    try {
      Operation operation =
          functions
              .projects()
              .locations()
              .functions()
              .create(
                  String.format("projects/%s/locations/%s", project, location),
                  new CloudFunction()
                      .setDescription("Archiver function for " + topicName)
                      .setEntryPoint("Archiver")
                      .setEnvironmentVariables(
                          Collections.singletonMap("BUCKET_NAME", ITConfig.STORAGE_BUCKET_NAME))
                      .setEventTrigger(
                          new EventTrigger()
                              .setEventType("google.pubsub.topic.publish")
                              .setResource(topicName)
                              .setService("pubsub.googleapis.com"))
                      .setIngressSettings("ALLOW_ALL")
                      .setName(
                          String.format(
                              "projects/%s/locations/%s/functions/%s",
                              project, location, functionId))
                      .setRuntime("go111")
                      .setServiceAccountEmail(ITConfig.FUNCTIONS_SERVICE_ACCOUNT_EMAIL)
                      .setSourceUploadUrl(sourceUrl))
              .execute();
      while (operation.getDone() == null || !operation.getDone().booleanValue()) {
        logger.info("Waiting for function to be created...");
        Thread.sleep(1000L);
        operation = functions.operations().get(operation.getName()).execute();
      }
      if (operation.getError() != null) {
        throw new RuntimeException(operation.getError().getMessage());
      }
      logger.info("Created function: " + operation.getResponse());
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_CONFLICT) {
        // Already exists, just ignore.
        logger.info("Cloud function already exists. Using existing function.");
      } else {
        throw e;
      }
    }
  }

  private static String uploadSourceCode() throws Exception {
    Charset utf8 = Charset.forName("UTF8");
    ByteArrayOutputStream zippedCode = new ByteArrayOutputStream(1024);
    ZipOutputStream zos = new ZipOutputStream(zippedCode);
    for (String entry : new String[] {"archiver.go", "go.mod"}) {
      zos.putNextEntry(new ZipEntry(entry));
      try (Scanner scanner =
          new Scanner(ITArchiverTest.class.getClassLoader().getResourceAsStream("go/" + entry))) {
        while (scanner.hasNextLine()) {
          zos.write(scanner.nextLine().getBytes(utf8));
          zos.write("\n".getBytes(utf8));
        }
      }
      zos.closeEntry();
    }
    zos.close();

    GenerateUploadUrlResponse response =
        functions
            .projects()
            .locations()
            .functions()
            .generateUploadUrl(
                String.format(
                    "projects/%s/locations/%s", ITConfig.FUNCTIONS_PROJECT_ID, functionLocation),
                new GenerateUploadUrlRequest())
            .execute();
    NetHttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest request =
        requestFactory.buildPutRequest(
            new GenericUrl(response.getUploadUrl()),
            new ByteArrayContent("application/zip", zippedCode.toByteArray()));
    request.setHeaders(new HttpHeaders().set("x-goog-content-length-range", "0,104857600"));
    HttpResponse uploadResponse = request.execute();
    assertThat(uploadResponse.isSuccessStatusCode()).isTrue();
    return response.getUploadUrl();
  }

  @AfterClass
  public static void teardown() throws Exception {
    cleanupDatabase();
    cleanupTopic();
    cleanupBucket();
    cleanupCloudFunction();
  }

  private static void cleanupDatabase() {
    try {
      database.drop();
      spanner.close();
      logger.info("Dropped test database");
    } catch (Throwable t) {
      logger.log(Level.WARNING, "Could not drop test database", t);
    }
  }

  private static void cleanupTopic() {
    try {
      topicAdminClient.deleteTopic(
          String.format(
              "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID));
      topicAdminClient.close();
      logger.info("Dropped test topic");
    } catch (Throwable t) {
      logger.log(Level.WARNING, "Could not delete test topic", t);
    }
  }

  private static void cleanupBucket() {
    try {
      Bucket bucket = storage.get(ITConfig.STORAGE_BUCKET_NAME);
      for (Blob blob : bucket.list().iterateAll()) {
        blob.delete();
      }
      storage.delete(ITConfig.STORAGE_BUCKET_NAME);
      logger.info("Dropped test bucket");
    } catch (Throwable t) {
      logger.log(Level.WARNING, "Could not delete test bucket", t);
    }
  }

  private static void cleanupCloudFunction() {
    try {
      Operation operation =
          functions
              .projects()
              .locations()
              .functions()
              .delete(
                  String.format(
                      "projects/%s/locations/%s/functions/%s",
                      ITConfig.FUNCTIONS_PROJECT_ID,
                      functionLocation,
                      ITConfig.FUNCTIONS_FUNCTION_ID))
              .execute();
      while (operation.getDone() == null || !operation.getDone().booleanValue()) {
        logger.info("Waiting for function to be deleted...");
        Thread.sleep(1000L);
        operation = functions.operations().get(operation.getName()).execute();
      }
      if (operation.getError() != null) {
        throw new RuntimeException(operation.getError().getMessage());
      }
      logger.info("Deleted function: " + operation.getResponse());
    } catch (Throwable t) {
      logger.log(Level.WARNING, "Could not delete test function", t);
    }
  }

  @Test
  public void testEventPublisher() throws Exception {
    final Set<BlobId> expectedBlobIds = Collections.synchronizedSet(new HashSet<>());
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    SpannerTableChangeCapturer capturer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(database.getId(), "NUMBERS"))
            .setPollInterval(Duration.ofMillis(50L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setCreateTableIfNotExists()
                    .build())
            .build();
    SpannerTableChangeEventPublisher eventPublisher =
        SpannerTableChangeEventPublisher.newBuilder(capturer)
            .setTopicName(
                String.format(
                    "projects/%s/topics/%s", ITConfig.PUBSUB_PROJECT_ID, ITConfig.PUBSUB_TOPIC_ID))
            .setCredentials(ITConfig.getPubSubCredentials())
            .setListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    expectedBlobIds.add(
                        BlobId.of(
                            ITConfig.STORAGE_BUCKET_NAME,
                            table.getDatabaseId().getName()
                                + "/"
                                + table.getTable()
                                + "-"
                                + commitTimestamp.toString()
                                + "-"
                                + messageId));
                    latch.countDown();
                  }
                })
            .build();
    eventPublisher.start();
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

    logger.log(Level.INFO, "Waiting for changes to be written to Cloud Storage...");
    // Wait until all messages have been published.
    assertThat(latch.await(10L, TimeUnit.SECONDS)).isTrue();
    // Start polling for the expected files.
    Stopwatch watch = Stopwatch.createStarted();
    List<Blob> blobs;
    do {
      blobs = storage.get(expectedBlobIds);
    } while (blobs.contains(null) && watch.elapsed(TimeUnit.SECONDS) <= 20L);
    assertThat(blobs).doesNotContain(null);
    assertThat(blobs).hasSize(expectedBlobIds.size());
    logger.log(Level.INFO, "Changes have been written to Cloud Storage");

    eventPublisher.stop();
    assertThat(eventPublisher.awaitTermination(10L, TimeUnit.SECONDS)).isTrue();
  }
}
