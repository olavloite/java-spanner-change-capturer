package com.google.cloud.spanner.capturer;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;

public class SpannerUtils {
  public static final String TS_QUERY =
      "SELECT COLUMN_NAME, OPTION_NAME, OPTION_VALUE FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE TABLE_NAME = @table";

  public static String getTimestampColumn(DatabaseClient client, String table) {
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(Statement.newBuilder(TS_QUERY).bind("table").to(table).build())) {
      while (rs.next()) {
        if (rs.getString("OPTION_NAME").equals("allow_commit_timestamp")) {
          if (rs.getString("OPTION_VALUE").equals("TRUE")) {
            return rs.getString("COLUMN_NAME");
          }
        }
      }
    }
    throw SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format(
            "Table %s does not contain a column with option allow_commit_timestamp=true", table));
  }
}
