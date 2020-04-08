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

package com.google.cloud.spanner.poller;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.common.annotations.VisibleForTesting;

/** Utils for getting commonly needed schema information from a Spanner database. */
public class SpannerUtils {
  /** Query for getting the column of a table that holds the commit timestamp. */
  @VisibleForTesting
  public static final String FIND_COMMIT_TIMESTAMP_COLUMN_QUERY =
      "SELECT COLUMN_NAME, OPTION_NAME, OPTION_VALUE FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE TABLE_NAME = @table";

  /** Returns the name of the commit timestamp column of the given table. */
  public static String getTimestampColumn(DatabaseClient client, String table) {
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(FIND_COMMIT_TIMESTAMP_COLUMN_QUERY)
                    .bind("table")
                    .to(table)
                    .build())) {
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
