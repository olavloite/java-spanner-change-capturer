package com.google.cloud.spanner.cdc;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.services.cloudfunctions.v1.CloudFunctions;
import com.google.api.services.cloudfunctions.v1.CloudFunctionsScopes;
import com.google.api.services.cloudfunctions.v1.model.CloudFunction;
import com.google.api.services.cloudfunctions.v1.model.EventTrigger;
import com.google.api.services.cloudfunctions.v1.model.ListFunctionsResponse;
import com.google.api.services.cloudfunctions.v1.model.Operation;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloudFunctionsTest {

  @Test
  public void test() throws Exception {
    GoogleCredentials credentials =
        GoogleCredentials.fromStream(
                new FileInputStream("/home/loite/CloudSpannerKeys/testspanner.json"))
            .createScoped(CloudFunctionsScopes.all());
    CloudFunctions functions =
        new CloudFunctions(
            GoogleNetHttpTransport.newTrustedTransport(),
            JacksonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials));

    ListFunctionsResponse response =
        functions
            .projects()
            .locations()
            .functions()
            .list("projects/testspanner-159712/locations/-")
            .execute();
    for (Object o : response.values()) {
      System.out.println(o);
    }

    try {
      Operation op =
          functions
              .projects()
              .locations()
              .functions()
              .delete("projects/testspanner-159712/locations/us-east1/functions/my-first-function")
              .execute();
      while (op.getDone() == null || !op.getDone().booleanValue()) {
        System.out.println("waiting for function to be deleted...");
        Thread.sleep(1000L);
        op = functions.operations().get(op.getName()).execute();
      }
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != StatusCode.Code.NOT_FOUND.getHttpStatusCode()) {
        throw e;
      }
    }

    Operation operation =
        functions
            .projects()
            .locations()
            .functions()
            .create(
                "projects/testspanner-159712/locations/us-east1",
                new CloudFunction()
                    .setDescription("Test Function")
                    .setEntryPoint("Archiver")
                    .setEventTrigger(
                        new EventTrigger()
                            .setEventType("google.pubsub.topic.publish")
                            .setResource("projects/testspanner-159712/topics/test-topic")
                            .setService("pubsub.googleapis.com"))
                    .setIngressSettings("ALLOW_ALL")
                    .setName(
                        "projects/testspanner-159712/locations/us-east1/functions/my-first-function")
                    .setRuntime("go111")
                    .setServiceAccountEmail(
                        "testspanner@testspanner-159712.iam.gserviceaccount.com")
                    .setSourceArchiveUrl("gs://spanner-cdc-functions/archiver-function.zip"))
            .execute();
    while (operation.getDone() == null || !operation.getDone().booleanValue()) {
      System.out.println("waiting for function to be created...");
      Thread.sleep(1000L);
      operation = functions.operations().get(operation.getName()).execute();
    }
    if (operation.getError() != null) {
      throw new RuntimeException(operation.getError().getMessage());
    }
    System.out.println(operation.getResponse());
  }
}
