package com.google.cloud.spanner.cdc.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudfunctions.v1.CloudFunctions;
import com.google.api.services.cloudfunctions.v1.model.GenerateUploadUrlRequest;
import com.google.api.services.cloudfunctions.v1.model.GenerateUploadUrlResponse;
import com.google.auth.http.HttpCredentialsAdapter;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ITUploaderTest {
  private static CloudFunctions functions;
  private static String functionLocation = "us-east1";

  @BeforeClass
  public static void setup() throws Exception {
    functions =
        new CloudFunctions(
            GoogleNetHttpTransport.newTrustedTransport(),
            JacksonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(ITConfig.getCloudFunctionsCredentials()));
  }

  @Test
  public void testUploadSourceCode() throws Exception {
    Charset utf8 = Charset.forName("UTF8");
    ByteArrayOutputStream zippedCode = new ByteArrayOutputStream(1024);
    ZipOutputStream zos = new ZipOutputStream(zippedCode);
    for (String entry : new String[] {"archiver.go", "go.mod"}) {
      zos.putNextEntry(new ZipEntry(entry));
      try (Scanner scanner =
          new Scanner(getClass().getClassLoader().getResourceAsStream("go/" + entry))) {
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
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder(new URI(response.getUploadUrl()))
            .PUT(BodyPublishers.ofByteArray(zippedCode.toByteArray()))
            .header("content-type", "application/zip")
            .header("x-goog-content-length-range", "0,104857600")
            .build();
    HttpResponse<String> uploadResponse = client.send(request, BodyHandlers.ofString());
    assertThat(uploadResponse.statusCode()).isEqualTo(200);
  }
}
