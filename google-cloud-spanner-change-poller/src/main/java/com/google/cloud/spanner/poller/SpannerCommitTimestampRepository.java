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

import com.google.api.client.util.Preconditions;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import java.util.Collections;

/**
 * {@link CommitTimestampRepository} that stores the last seen commit timestamp for a table in a
 * Cloud Spanner database table. The default table definition to use is
 *
 * <pre>
 * CREATE TABLE LAST_SEEN_COMMIT_TIMESTAMPS (
 *        TABLE_NAME STRING(MAX) NOT NULL,
 *        LAST_SEEN_COMMIT_TIMESTAMP TIMESTAMP NOT NULL
 * ) PRIMARY KEY (TABLE_NAME)
 * </pre>
 *
 * The table name and column names are configurable.
 */
public class SpannerCommitTimestampRepository implements CommitTimestampRepository {
  static final String DEFAULT_TABLE_NAME = "LAST_SEEN_COMMIT_TIMESTAMPS";
  static final String DEFAULT_TABLE_NAME_COLUMN_NAME = "TABLE_NAME";
  static final String DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME = "LAST_SEEN_COMMIT_TIMESTAMP";
  static final String FIND_TABLE_STATEMENT =
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=@table";
  static final String FIND_COLUMNS_STATEMENT =
      "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@table ORDER BY ORDINAL_POSITION";
  static final String FIND_PK_COLUMNS_STATEMENT =
      "SELECT INDEX_COLUMNS.COLUMN_NAME\n"
          + "FROM INFORMATION_SCHEMA.INDEXES\n"
          + "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS \n"
          + "            ON  INDEXES.TABLE_CATALOG=INDEX_COLUMNS.TABLE_CATALOG\n"
          + "            AND INDEXES.TABLE_SCHEMA=INDEX_COLUMNS.TABLE_SCHEMA\n"
          + "            AND INDEXES.TABLE_NAME=INDEX_COLUMNS.TABLE_NAME\n"
          + "            AND INDEXES.INDEX_NAME=INDEX_COLUMNS.INDEX_NAME\n"
          + "WHERE INDEXES.TABLE_NAME=@table AND INDEXES.INDEX_TYPE='PRIMARY_KEY'";

  public static class Builder {
    private final DatabaseClient client;
    private String commitTimestampsTable = DEFAULT_TABLE_NAME;
    private String tableCol = DEFAULT_TABLE_NAME_COLUMN_NAME;
    private String tsCol = DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME;

    private Builder(DatabaseClient client) {
      this.client = Preconditions.checkNotNull(client);
    }

    public Builder setCommitTimestampsTable(String table) {
      this.commitTimestampsTable = Preconditions.checkNotNull(table);
      return this;
    }

    public Builder setTableNameColumn(String column) {
      this.tableCol = Preconditions.checkNotNull(column);
      return this;
    }

    public Builder setCommitTimestampColumn(String column) {
      this.tsCol = Preconditions.checkNotNull(column);
      return this;
    }

    public SpannerCommitTimestampRepository build() {
      return new SpannerCommitTimestampRepository(this);
    }
  }

  public static Builder newBuilder(DatabaseClient client) {
    return new Builder(client);
  }

  private final DatabaseClient client;
  private final String commitTimestampsTable;
  private final String tableCol;
  private final String tsCol;
  private final Iterable<String> tsColumns;
  private boolean initialized = false;

  private SpannerCommitTimestampRepository(Builder builder) {
    this.client = builder.client;
    this.commitTimestampsTable = builder.commitTimestampsTable;
    this.tableCol = builder.tableCol;
    this.tsCol = builder.tsCol;
    this.tsColumns = Collections.singleton(builder.tsCol);
  }

  /** Checks that the table is present and contains the actually expected columns. */
  private void initialize() {
    Statement statement =
        Statement.newBuilder(FIND_TABLE_STATEMENT).bind("table").to(commitTimestampsTable).build();
    try (ResultSet rs = client.singleUse().executeQuery(statement)) {
      if (!rs.next()) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.NOT_FOUND, String.format("Table %s not found", commitTimestampsTable));
      }
    }
    // Table exists, check that it contains the expected columns.
    verifyTable();
    initialized = true;
  }

  private void verifyTable() {
    Statement columnsStatement =
        Statement.newBuilder(FIND_COLUMNS_STATEMENT)
            .bind("table")
            .to(commitTimestampsTable)
            .build();
    boolean foundTableCol = false;
    boolean foundTsCol = false;
    try (ResultSet rs = client.singleUse().executeQuery(columnsStatement)) {
      while (rs.next()) {
        String col = rs.getString("COLUMN_NAME");
        if (col.equalsIgnoreCase(tableCol)) {
          if (!rs.getString("SPANNER_TYPE").startsWith("STRING")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Table name column %s is not of type STRING, but of type %s",
                    tableCol, rs.getString("SPANNER_TYPE")));
          }
          foundTableCol = true;
        } else if (col.equalsIgnoreCase(tsCol)) {
          if (!rs.getString("SPANNER_TYPE").equals("TIMESTAMP")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Commit timestamp column %s is not of type TIMESTAMP, but of type %s",
                    tsCol, rs.getString("SPANNER_TYPE")));
          }
          foundTsCol = true;
        }
      }
    }
    if (!foundTableCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Table name column %s not found", tableCol));
    }
    if (!foundTsCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Commit timestamp column %s not found", tsCol));
    }

    // Verify that the table name column is the primary key.
    Statement pkStatement =
        Statement.newBuilder(FIND_PK_COLUMNS_STATEMENT)
            .bind("table")
            .to(commitTimestampsTable)
            .build();
    try (ResultSet rs = client.singleUse().executeQuery(pkStatement)) {
      if (rs.next()) {
        if (!tableCol.equalsIgnoreCase(rs.getString(0))) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              String.format(
                  "Column %s should be the only column of the primary key of the table %s, but instead column %s was found as the first column of the primary key.",
                  tableCol, commitTimestampsTable, rs.getString(0)));
        }
      } else {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.NOT_FOUND,
            "Table %s does not have a primary key. The table name column must be the primary key of the table.");
      }
    }
  }

  @Override
  public Timestamp get(String table) {
    if (!initialized) {
      initialize();
    }
    Struct row = client.singleUse().readRow(commitTimestampsTable, Key.of(table), tsColumns);
    if (row == null) {
      return Timestamp.parseTimestamp("0001-01-01T00:00:00Z");
    }
    return row.getTimestamp(0);
  }

  @Override
  public void set(String table, Timestamp commitTimestamp) {
    if (!initialized) {
      initialize();
    }
    client.writeAtLeastOnce(
        Collections.singleton(
            Mutation.newInsertOrUpdateBuilder(commitTimestampsTable)
                .set(tableCol)
                .to(table)
                .set(tsCol)
                .to(commitTimestamp)
                .build()));
  }
}
