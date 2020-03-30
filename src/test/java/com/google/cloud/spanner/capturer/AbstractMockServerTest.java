/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.capturer;

import com.google.api.core.ApiFunction;
import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public abstract class AbstractMockServerTest {
  private static final Statement COLUMN_OPTIONS_FOO_STATEMENT =
      Statement.newBuilder(SpannerUtils.TS_QUERY).bind("table").to("Foo").build();
  static final Statement SELECT_FOO_STATEMENT =
      Statement.newBuilder(
              "SELECT * FROM `Foo` WHERE `LastModified`>@prevCommitTimestamp ORDER BY `LastModified`")
          .bind("prevCommitTimestamp")
          .to(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"))
          .build();
  public static final int SELECT_FOO_ROW_COUNT = 10;

  // COLUMN_NAME, OPTION_NAME, OPTION_VALUE
  private static final ResultSetMetadata COLUMNS_OPTIONS_FOO_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("OPTION_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("OPTION_VALUE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet COLUMNS_OPTIONS_FOO_RESULT =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("LastModified").build())
                  .addValues(Value.newBuilder().setStringValue("allow_commit_timestamp").build())
                  .addValues(Value.newBuilder().setStringValue("TRUE").build())
                  .build())
          .setMetadata(COLUMNS_OPTIONS_FOO_METADATA)
          .build();

  static MockSpannerServiceImpl mockSpanner;
  private static Server server;
  private static InetSocketAddress address;
  public static Spanner spanner;

  @BeforeClass
  public static void startStaticServer() throws IOException {
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0D); // We don't want any unpredictable aborted transactions.
    address = new InetSocketAddress("localhost", 0);
    server = NettyServerBuilder.forAddress(address).addService(mockSpanner).build().start();
    mockSpanner.putStatementResult(
        StatementResult.query(COLUMN_OPTIONS_FOO_STATEMENT, COLUMNS_OPTIONS_FOO_RESULT));
    mockSpanner.putStatementResult(
        StatementResult.queryAndThen(
            SELECT_FOO_STATEMENT,
            new RandomResultSetGenerator(SELECT_FOO_ROW_COUNT).generate(),
            new RandomResultSetGenerator(0).generate()));
    spanner = createSpanner();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    spanner.close();
    server.shutdown();
    server.awaitTermination();
  }

  @Before
  public void setupResults() {
    mockSpanner.reset();
  }

  @SuppressWarnings("rawtypes")
  private static Spanner createSpanner() {
    return SpannerOptions.newBuilder()
        .setProjectId("project-id")
        .setCredentials(NoCredentials.getInstance())
        .setHost("http://localhost:" + server.getPort())
        .setChannelConfigurator(
            new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
              @Override
              public ManagedChannelBuilder apply(ManagedChannelBuilder input) {
                return input.usePlaintext();
              }
            })
        .build()
        .getService();
  }
}
