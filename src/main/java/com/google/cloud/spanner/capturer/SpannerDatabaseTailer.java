package com.google.cloud.spanner.capturer;

import com.google.api.client.util.Preconditions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.capturer.SpannerTableChangeCapturer.RowChangeCallback;
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
import org.threeten.bp.Duration;

/** Change capturer for one or more tables of a Spanner database. */
public class SpannerDatabaseTailer implements SpannerDatabaseChangeCapturer {
  private static final String ALL_TABLE_NAMES_STATEMENT =
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME NOT IN UNNEST(@excluded) AND TABLE_SCHEMA=@schema AND TABLE_CATALOG=@catalog";

  public static class Builder {
    private final DatabaseClient client;
    private boolean allTables = false;
    private List<String> tables = new ArrayList<>();
    private List<String> excludedTables = new ArrayList<>();
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;

    private Builder(DatabaseClient client) {
      this.client = Preconditions.checkNotNull(client);
      this.commitTimestampRepository = SpannerCommitTimestampRepository.newBuilder(client).build();
      this.excludedTables.add(SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME);
    }

    public Builder setAllTables() {
      Preconditions.checkState(
          tables.isEmpty(), "Cannot use specific tables in combination with allTables");
      this.allTables = true;
      return this;
    }

    public Builder appendTable(String table) {
      Preconditions.checkState(
          !allTables, "Cannot use specific tables in combination with allTables");
      this.tables.add(Preconditions.checkNotNull(table));
      return this;
    }

    public Builder setTables(String... tables) {
      Preconditions.checkState(
          !allTables, "Cannot use specific tables in combination with allTables");
      this.tables = Arrays.asList(tables);
      return this;
    }

    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    public Builder setPollInterval(Duration interval) {
      this.pollInterval = Preconditions.checkNotNull(interval);
      return this;
    }

    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    public SpannerDatabaseTailer build() {
      return new SpannerDatabaseTailer(this);
    }
  }

  public static Builder newBuilder(DatabaseClient client) {
    return new Builder(client);
  }

  private boolean started;
  private boolean stopped;
  private final DatabaseClient client;
  private final ImmutableList<String> tables;
  private final List<String> excludedTables;
  private final Map<String, SpannerTableChangeCapturer> capturers;

  private SpannerDatabaseTailer(Builder builder) {
    this.client = builder.client;
    this.excludedTables = builder.excludedTables;
    if (builder.allTables) {
      tables = allTableNames(builder.client);
    } else {
      tables = ImmutableList.copyOf(builder.tables);
    }
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
        Statement.newBuilder(ALL_TABLE_NAMES_STATEMENT)
            .bind("excluded")
            .toStringArray(excludedTables)
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
  public void stop() {
    Preconditions.checkState(started, "This DatabaseTailer has not been started");
    Preconditions.checkState(!stopped, "This DatabaseTailer has already been stopped");
    stopped = true;
    for (SpannerTableChangeCapturer c : capturers.values()) {
      c.stop();
    }
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
