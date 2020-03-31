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

import com.google.api.client.util.Preconditions;
import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.cdc.SpannerTableChangeCapturer.RowChangeCallback;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.threeten.bp.Duration;

/** Change capturer for one or more tables of a Spanner database. */
public class SpannerDatabaseTailer implements SpannerDatabaseChangeCapturer {
  /** Lists all tables with a commit timestamp column. */
  static final String LIST_TABLE_NAMES_STATEMENT =
      "SELECT TABLE_NAME\n"
          + "FROM INFORMATION_SCHEMA.TABLES\n"
          + "WHERE TABLE_NAME NOT IN UNNEST(@excluded)\n"
          + "AND (@allTables=TRUE OR TABLE_NAME IN UNNEST(@included))\n"
          + "AND TABLE_SCHEMA=@schema\n"
          + "AND TABLE_CATALOG=@catalog\n"
          + "AND TABLE_NAME IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE OPTION_NAME='allow_commit_timestamp' AND OPTION_VALUE='TRUE')";

  public static class Builder {
    private final DatabaseClient client;
    private boolean allTables = false;
    private List<String> includedTables = new ArrayList<>();
    private List<String> excludedTables = new ArrayList<>();
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;

    private Builder(DatabaseClient client) {
      this.client = Preconditions.checkNotNull(client);
      this.commitTimestampRepository = SpannerCommitTimestampRepository.newBuilder(client).build();
    }

    /**
     * Instructs the {@link SpannerDatabaseTailer} to emit change events for all tables in the
     * database that have a column with option ALLOW_COMMIT_TIMESTAMP=TRUE. Tables that don't have a
     * commit timestamp column are automatically ignored. Additional tables can be excluded by
     * calling {@link #excludeTables(String...)}.
     */
    public Builder setAllTables() {
      Preconditions.checkState(
          includedTables.isEmpty(), "Cannot include specific tables in combination with allTables");
      this.allTables = true;
      return this;
    }

    /**
     * Instructs the {@link SpannerDatabaseTailer} to only emit changes for these specific tables.
     * This option cannot be used in combination with {@link #setAllTables()}. If you both exclude
     * and include the same table, the exclusion will get priority and the table will not be
     * included.
     */
    public Builder includeTables(String... tables) {
      Preconditions.checkState(
          !allTables, "Cannot include specific tables in combination with allTables");
      includedTables.addAll(Arrays.asList(tables));
      return this;
    }

    /**
     * Instructs the {@link SpannerDatabaseTailer} to exclude these tables from change events. This
     * option can be used in combination with {@link #setAllTables()} to include all tables except
     * for a specfic list.
     */
    public Builder excludeTables(String... excludedTables) {
      this.excludedTables.addAll(Arrays.asList(excludedTables));
      return this;
    }

    /**
     * Sets a specific {@link CommitTimestampRepository} to use for the {@link
     * SpannerDatabaseTailer}. The default will use a {@link SpannerCommitTimestampRepository} which
     * stores the last seen commit timestamp in a table named LAST_SEEN_COMMIT_TIMESTAMPS in the
     * Spanner database that this {@link SpannerDatabaseTailer} is monitoring.
     */
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    /**
     * Sets the poll interval to use for this {@link SpannerDatabaseTailer}. The default is 1
     * second.
     */
    public Builder setPollInterval(Duration interval) {
      this.pollInterval = Preconditions.checkNotNull(interval);
      return this;
    }

    /**
     * Sets a specific {@link ScheduledExecutorService} to use for this {@link
     * SpannerDatabaseTailer}. The default will use a {@link ScheduledThreadPoolExecutor} with a
     * core size equal to the number of tables that is being monitored.
     */
    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    /** Creates a {@link SpannerDatabaseTailer} from this builder. */
    public SpannerDatabaseTailer build() {
      return new SpannerDatabaseTailer(this);
    }
  }

  /** Creates a builder for a {@link SpannerDatabaseTailer}. */
  public static Builder newBuilder(DatabaseClient client) {
    return new Builder(client);
  }

  private boolean started;
  private boolean stopped;
  private final DatabaseClient client;
  private final boolean allTables;
  private final ImmutableList<String> tables;
  private final ImmutableList<String> includedTables;
  private final ImmutableList<String> excludedTables;
  private final Map<String, SpannerTableChangeCapturer> capturers;

  private SpannerDatabaseTailer(Builder builder) {
    this.client = builder.client;
    this.allTables = builder.allTables;
    this.includedTables = ImmutableList.copyOf(builder.includedTables);
    this.excludedTables = ImmutableList.copyOf(builder.excludedTables);
    this.tables = allTableNames(client);
    ScheduledExecutorService executor;
    if (builder.executor == null) {
      executor = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(tables.size()));
    } else {
      executor = builder.executor;
    }
    capturers = new HashMap<>(tables.size());
    for (String table : tables) {
      capturers.put(
          table,
          SpannerTableTailer.newBuilder(builder.client, table)
              .setCommitTimestampRepository(builder.commitTimestampRepository)
              .setPollInterval(builder.pollInterval)
              .setExecutor(executor)
              .build());
    }
  }

  private ImmutableList<String> allTableNames(DatabaseClient client) {
    Statement statement =
        Statement.newBuilder(LIST_TABLE_NAMES_STATEMENT)
            .bind("excluded")
            .toStringArray(excludedTables)
            .bind("allTables")
            .to(allTables)
            .bind("included")
            .toStringArray(includedTables)
            .bind("schema")
            .to("")
            .bind("catalog")
            .to("")
            .build();
    return client
        .singleUse()
        .executeQueryAsync(statement)
        .toList(
            new Function<StructReader, String>() {
              @Override
              public String apply(StructReader input) {
                return input.getString(0);
              }
            });
  }

  @Override
  public void start(RowChangeCallback callback) {
    Preconditions.checkState(!started, "This DatabaseTailer has already been started");
    started = true;
    for (SpannerTableChangeCapturer c : capturers.values()) {
      c.start(callback);
    }
  }

  @Override
  public ApiFuture<Void> stopAsync() {
    Preconditions.checkState(started, "This DatabaseTailer has not been started");
    Preconditions.checkState(!stopped, "This DatabaseTailer has already been stopped");
    stopped = true;
    List<ApiFuture<Void>> futures = new ArrayList<>(capturers.size());
    for (SpannerTableChangeCapturer c : capturers.values()) {
      futures.add(c.stopAsync());
    }
    return ApiFutures.transform(
        ApiFutures.allAsList(futures),
        new ApiFunction<List<Void>, Void>() {
          @Override
          public Void apply(List<Void> input) {
            return null;
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public DatabaseClient getDatabaseClient() {
    return client;
  }

  @Override
  public ImmutableList<String> getTables() {
    return tables;
  }

  @Override
  public SpannerTableChangeCapturer getCapturer(String table) {
    return capturers.get(table);
  }
}
