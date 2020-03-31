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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.cdc.SpannerToAvro.SchemaSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerToAvroTest {
  static final String SCHEMA_QUERY =
      "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@table ORDER BY ORDINAL_POSITION";
  private static final Type SCHEMA_ROW_TYPE =
      Type.struct(
          StructField.of("COLUMN_NAME", Type.string()),
          StructField.of("SPANNER_TYPE", Type.string()),
          StructField.of("IS_NULLABLE", Type.string()));

  private static ResultSet createSchemaResultSet() {
    return ResultSets.forRows(
        SCHEMA_ROW_TYPE,
        Arrays.asList(
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C1")
                .set("SPANNER_TYPE")
                .to("INT64")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C2")
                .set("SPANNER_TYPE")
                .to("INT64")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C3")
                .set("SPANNER_TYPE")
                .to("BOOL")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C4")
                .set("SPANNER_TYPE")
                .to("BOOL")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C5")
                .set("SPANNER_TYPE")
                .to("BYTES(24)")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C6")
                .set("SPANNER_TYPE")
                .to("BYTES(MAX)")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C7")
                .set("SPANNER_TYPE")
                .to("STRING(100)")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C8")
                .set("SPANNER_TYPE")
                .to("STRING(MAX)")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C9")
                .set("SPANNER_TYPE")
                .to("FLOAT64")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C10")
                .set("SPANNER_TYPE")
                .to("FLOAT64")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C11")
                .set("SPANNER_TYPE")
                .to("DATE")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C12")
                .set("SPANNER_TYPE")
                .to("DATE")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C13")
                .set("SPANNER_TYPE")
                .to("TIMESTAMP")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C14")
                .set("SPANNER_TYPE")
                .to("TIMESTAMP")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),

            // ARRAY types.
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C15")
                .set("SPANNER_TYPE")
                .to("ARRAY<INT64>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C16")
                .set("SPANNER_TYPE")
                .to("ARRAY<INT64>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C17")
                .set("SPANNER_TYPE")
                .to("ARRAY<BOOL>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C18")
                .set("SPANNER_TYPE")
                .to("ARRAY<BOOL>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C19")
                .set("SPANNER_TYPE")
                .to("ARRAY<BYTES(24)>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C20")
                .set("SPANNER_TYPE")
                .to("ARRAY<BYTES(MAX)>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C21")
                .set("SPANNER_TYPE")
                .to("ARRAY<STRING(100)>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C22")
                .set("SPANNER_TYPE")
                .to("ARRAY<STRING(MAX)>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C23")
                .set("SPANNER_TYPE")
                .to("ARRAY<FLOAT64>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C24")
                .set("SPANNER_TYPE")
                .to("ARRAY<FLOAT64>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C25")
                .set("SPANNER_TYPE")
                .to("ARRAY<DATE>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C26")
                .set("SPANNER_TYPE")
                .to("ARRAY<DATE>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C27")
                .set("SPANNER_TYPE")
                .to("ARRAY<TIMESTAMP>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("C28")
                .set("SPANNER_TYPE")
                .to("ARRAY<TIMESTAMP>")
                .set("IS_NULLABLE")
                .to("YES")
                .build()));
  }

  @Test
  public void testConvertTableToSchemaSet() {
    SchemaSet set =
        SpannerToAvro.convertTableToSchemaSet(
            "FOO", "NAMESPACE", createSchemaResultSet(), "commitTimestamp");
    Schema schema = set.avroSchema();
    assertThat(schema.getField("C1").schema()).isEqualTo(SchemaBuilder.builder().longType());
    assertThat(schema.getField("C1").schema().isNullable()).isFalse();
    assertThat(schema.getField("C2").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().longType().endUnion());
    assertThat(schema.getField("C2").schema().isNullable()).isTrue();

    assertThat(schema.getField("C3").schema()).isEqualTo(SchemaBuilder.builder().booleanType());
    assertThat(schema.getField("C3").schema().isNullable()).isFalse();
    assertThat(schema.getField("C4").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().booleanType().endUnion());
    assertThat(schema.getField("C4").schema().isNullable()).isTrue();

    assertThat(schema.getField("C5").schema()).isEqualTo(SchemaBuilder.builder().bytesType());
    assertThat(schema.getField("C5").schema().isNullable()).isFalse();
    assertThat(schema.getField("C6").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion());
    assertThat(schema.getField("C6").schema().isNullable()).isTrue();

    assertThat(schema.getField("C7").schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField("C7").schema().isNullable()).isFalse();
    assertThat(schema.getField("C8").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField("C8").schema().isNullable()).isTrue();

    assertThat(schema.getField("C9").schema()).isEqualTo(SchemaBuilder.builder().doubleType());
    assertThat(schema.getField("C9").schema().isNullable()).isFalse();
    assertThat(schema.getField("C10").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().doubleType().endUnion());
    assertThat(schema.getField("C10").schema().isNullable()).isTrue();

    // DATE and TIMESTAMP are both handled as STRING.
    assertThat(schema.getField("C11").schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField("C11").schema().isNullable()).isFalse();
    assertThat(schema.getField("C12").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField("C12").schema().isNullable()).isTrue();

    assertThat(schema.getField("C13").schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField("C13").schema().isNullable()).isFalse();
    assertThat(schema.getField("C14").schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField("C14").schema().isNullable()).isTrue();

    // ARRAY types.
    assertThat(schema.getField("C15").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .longType()
                .endUnion());
    assertThat(schema.getField("C15").schema().isNullable()).isFalse();
    assertThat(schema.getField("C16").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .longType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C16").schema().isNullable()).isTrue();

    assertThat(schema.getField("C17").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .booleanType()
                .endUnion());
    assertThat(schema.getField("C17").schema().isNullable()).isFalse();
    assertThat(schema.getField("C18").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .booleanType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C18").schema().isNullable()).isTrue();

    assertThat(schema.getField("C19").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .bytesType()
                .endUnion());
    assertThat(schema.getField("C19").schema().isNullable()).isFalse();
    assertThat(schema.getField("C20").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .bytesType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C20").schema().isNullable()).isTrue();

    assertThat(schema.getField("C21").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField("C21").schema().isNullable()).isFalse();
    assertThat(schema.getField("C22").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C22").schema().isNullable()).isTrue();

    assertThat(schema.getField("C23").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .doubleType()
                .endUnion());
    assertThat(schema.getField("C23").schema().isNullable()).isFalse();
    assertThat(schema.getField("C24").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .doubleType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C24").schema().isNullable()).isTrue();

    // DATE and TIMESTAMP are both handled as STRING.
    assertThat(schema.getField("C25").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField("C25").schema().isNullable()).isFalse();
    assertThat(schema.getField("C26").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C26").schema().isNullable()).isTrue();

    assertThat(schema.getField("C27").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField("C27").schema().isNullable()).isFalse();
    assertThat(schema.getField("C28").schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField("C28").schema().isNullable()).isTrue();
  }

  @Test
  public void testMakeRecord() throws IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    ReadContext context = mock(ReadContext.class);
    when(client.singleUse()).thenReturn(context);
    when(context.executeQuery(
            Statement.newBuilder(SpannerUtils.TS_QUERY).bind("table").to("FOO").build()))
        .thenReturn(createTimestampColumnResultSet());
    when(context.executeQuery(
            Statement.newBuilder(SpannerToAvro.SCHEMA_QUERY).bind("table").to("FOO").build()))
        .thenReturn(createSchemaResultSet());
    SpannerToAvro converter = new SpannerToAvro(client, "FOO");

    Struct row =
        Struct.newBuilder()
            .set("C1")
            .to(1L)
            .set("C2")
            .to((Long) null)
            .set("C3")
            .to(true)
            .set("C4")
            .to((Boolean) null)
            .set("C5")
            .to(ByteArray.copyFrom("TEST"))
            .set("C6")
            .to((ByteArray) null)
            .set("C7")
            .to("TEST")
            .set("C8")
            .to((String) null)
            .set("C9")
            .to(3.14D)
            .set("C10")
            .to((Double) null)
            .set("C11")
            .to(Date.fromYearMonthDay(2020, 3, 31))
            .set("C12")
            .to((Date) null)
            .set("C13")
            .to(Timestamp.now())
            .set("C14")
            .to((Timestamp) null)
            // ARRAY types
            .set("C15")
            .toInt64Array(Arrays.asList(1L, null, 3L, null, 5L))
            .set("C16")
            .toInt64Array((long[]) null)
            .set("C17")
            .toBoolArray(Arrays.asList(true, null, false, null))
            .set("C18")
            .toBoolArray((boolean[]) null)
            .set("C19")
            .toBytesArray(
                Arrays.asList(ByteArray.copyFrom("TEST"), null, ByteArray.copyFrom("FOO"), null))
            .set("C20")
            .toBytesArray(null)
            .set("C21")
            .toStringArray(Arrays.asList("TEST", null, "FOO", null))
            .set("C22")
            .toStringArray(null)
            .set("C23")
            .toFloat64Array(Arrays.asList(3.14D, null, 6.626D, null))
            .set("C24")
            .toFloat64Array((double[]) null)
            .set("C25")
            .toDateArray(
                Arrays.asList(
                    Date.fromYearMonthDay(2020, 3, 31),
                    null,
                    Date.fromYearMonthDay(1970, 1, 1),
                    null))
            .set("C26")
            .toDateArray(null)
            .set("C27")
            .toTimestampArray(
                Arrays.asList(Timestamp.now(), null, Timestamp.ofTimeSecondsAndNanos(0, 0), null))
            .set("C28")
            .toTimestampArray(null)
            .build();

    ByteString data = converter.makeRecord(row);
    assertThat(data).isNotNull();

    // Read the data back in.
    SchemaSet set =
        SpannerToAvro.convertTableToSchemaSet(
            "FOO", "NAMESPACE", createSchemaResultSet(), "LastModifiedAt");
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data.newInput(), null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(set.avroSchema());
    GenericRecord record = reader.read(null, decoder);
    assertThat(record).isNotNull();
    assertThat(record.get("C1")).isEqualTo(1L);
  }

  private ResultSet createTimestampColumnResultSet() {
    return ResultSets.forRows(
        Type.struct(
            StructField.of("COLUMN_NAME", Type.string()),
            StructField.of("OPTION_NAME", Type.string()),
            StructField.of("OPTION_VALUE", Type.string())),
        Collections.singleton(
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("LastModifiedAt")
                .set("OPTION_NAME")
                .to("allow_commit_timestamp")
                .set("OPTION_VALUE")
                .to("TRUE")
                .build()));
  }
}