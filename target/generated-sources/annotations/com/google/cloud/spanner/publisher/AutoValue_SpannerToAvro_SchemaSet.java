

package com.google.cloud.spanner.publisher;

import com.google.common.collect.ImmutableMap;
import javax.annotation.Generated;
import org.apache.avro.Schema;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SpannerToAvro_SchemaSet extends SpannerToAvro.SchemaSet {

  private final Schema avroSchema;

  private final ImmutableMap<String, String> spannerSchema;

  private final String tsColName;

  AutoValue_SpannerToAvro_SchemaSet(
      Schema avroSchema,
      ImmutableMap<String, String> spannerSchema,
      String tsColName) {
    if (avroSchema == null) {
      throw new NullPointerException("Null avroSchema");
    }
    this.avroSchema = avroSchema;
    if (spannerSchema == null) {
      throw new NullPointerException("Null spannerSchema");
    }
    this.spannerSchema = spannerSchema;
    if (tsColName == null) {
      throw new NullPointerException("Null tsColName");
    }
    this.tsColName = tsColName;
  }

  @Override
  Schema avroSchema() {
    return avroSchema;
  }

  @Override
  ImmutableMap<String, String> spannerSchema() {
    return spannerSchema;
  }

  @Override
  public String tsColName() {
    return tsColName;
  }

  @Override
  public String toString() {
    return "SchemaSet{"
         + "avroSchema=" + avroSchema + ", "
         + "spannerSchema=" + spannerSchema + ", "
         + "tsColName=" + tsColName
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerToAvro.SchemaSet) {
      SpannerToAvro.SchemaSet that = (SpannerToAvro.SchemaSet) o;
      return (this.avroSchema.equals(that.avroSchema()))
           && (this.spannerSchema.equals(that.spannerSchema()))
           && (this.tsColName.equals(that.tsColName()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= avroSchema.hashCode();
    h$ *= 1000003;
    h$ ^= spannerSchema.hashCode();
    h$ *= 1000003;
    h$ ^= tsColName.hashCode();
    return h$;
  }

}
