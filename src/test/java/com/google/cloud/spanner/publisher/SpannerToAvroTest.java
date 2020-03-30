package com.google.cloud.spanner.publisher;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.publisher.SpannerToAvro.SchemaSet;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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

  @Test
  public void testConvertTableToSchemaSet() {
    ResultSet resultSet =
        ResultSets.forRows(
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
                    .build()));
    SchemaSet set =
        SpannerToAvro.convertTableToSchemaSet("FOO", "NAMESPACE", resultSet, "commitTimestamp");
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
  }
}
