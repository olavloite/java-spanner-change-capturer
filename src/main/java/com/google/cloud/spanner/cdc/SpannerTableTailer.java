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

package com.google.cloud.spanner.cdc;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.ReadyCallback;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Preconditions;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Implementation of the {@link SpannerTableChangeCapturer} interface that continuously polls a
 * table for changes based on a commit timestamp column in the table.
 */
public class SpannerTableTailer implements SpannerTableChangeCapturer {
  private static final Logger logger = Logger.getLogger(SpannerTableTailer.class.getName());
  static final String PK_QUERY =
      "SELECT * FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME = @table";
  static final String SCHEMA_QUERY =
      "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@table ORDER BY ORDINAL_POSITION";

  public static class Builder {
    private final DatabaseClient client;
    private final String table;
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;

    private Builder(DatabaseClient client, String table) {
      this.client = Preconditions.checkNotNull(client);
      this.table = Preconditions.checkNotNull(table);
      this.commitTimestampRepository = SpannerCommitTimestampRepository.newBuilder(client).build();
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

    public SpannerTableTailer build() {
      return new SpannerTableTailer(this);
    }
  }

  public static Builder newBuilder(DatabaseClient client, String table) {
    return new Builder(client, table);
  }

  // TODO: Check and warn if the commit timestamp column is not part of an index.

  private final Object lock = new Object();
  private boolean started = false;
  private SettableApiFuture<Void> stopFuture;
  private final DatabaseClient client;
  private final String table;
  private final CommitTimestampRepository commitTimestampRepository;
  private final Duration pollInterval;
  private final ScheduledExecutorService executor;
  private final boolean isOwnedExecutor;
  private Timestamp startedPollWithCommitTimestamp;
  private Timestamp lastSeenCommitTimestamp;

  private RowChangeCallback callback;
  private String commitTimestampColumn;
  private Statement.Builder statement;

  private SpannerTableTailer(Builder builder) {
    this.client = builder.client;
    this.table = builder.table;
    this.commitTimestampRepository = builder.commitTimestampRepository;
    this.pollInterval = builder.pollInterval;
    this.executor =
        builder.executor == null ? Executors.newScheduledThreadPool(1) : builder.executor;
    this.isOwnedExecutor = builder.executor == null;
  }

  @Override
  public DatabaseClient getDatabaseClient() {
    return client;
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public void start(RowChangeCallback callback) {
    Preconditions.checkState(!started, "This SpannerTailer has already been started");
    this.started = true;
    this.lastSeenCommitTimestamp = commitTimestampRepository.get(table);
    this.callback = callback;
    commitTimestampColumn = SpannerUtils.getTimestampColumn(client, table);
    statement =
        Statement.newBuilder(
            String.format(
                "SELECT * FROM `%s` WHERE `%s`>@prevCommitTimestamp ORDER BY `%s`",
                table, commitTimestampColumn, commitTimestampColumn));
    executor.schedule(new SpannerTailerRunner(), 0L, TimeUnit.MILLISECONDS);
  }

  @Override
  public ApiFuture<Void> stopAsync() {
    synchronized (lock) {
      Preconditions.checkState(started, "This SpannerTailer has not been started");
      Preconditions.checkState(stopFuture == null, "This SpannerTailer has already been stopped");
      stopFuture = SettableApiFuture.create();
      if (isOwnedExecutor) {
        executor.shutdown();
      }
      return stopFuture;
    }
  }

  class SpannerTailerCallback implements ReadyCallback {
    private final RowChangeCallback delegate;

    private SpannerTailerCallback(RowChangeCallback delegate) {
      this.delegate = delegate;
    }

    private void scheduleNextPollOrStop() {
      if (lastSeenCommitTimestamp.compareTo(startedPollWithCommitTimestamp) > 0) {
        commitTimestampRepository.set(table, lastSeenCommitTimestamp);
      }
      synchronized (lock) {
        if (stopFuture == null) {
          executor.schedule(
              new SpannerTailerRunner(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
          stopFuture.set(null);
        }
      }
    }

    @Override
    public CallbackResponse cursorReady(AsyncResultSet resultSet) {
      try {
        while (true) {
          synchronized (lock) {
            if (stopFuture != null) {
              scheduleNextPollOrStop();
              return CallbackResponse.DONE;
            }
          }
          switch (resultSet.tryNext()) {
            case DONE:
              scheduleNextPollOrStop();
              return CallbackResponse.DONE;
            case NOT_READY:
              return CallbackResponse.CONTINUE;
            case OK:
              Timestamp ts = resultSet.getTimestamp(commitTimestampColumn);
              delegate.rowChange(table, new RowImpl(resultSet), ts);
              lastSeenCommitTimestamp = ts;
              logger.log(
                  Level.FINE,
                  String.format("Saw commit timestamp %s", lastSeenCommitTimestamp.toString()));
              break;
          }
        }
      } catch (Throwable t) {
        logger.log(Level.WARNING, "Error processing change set", t);
        scheduleNextPollOrStop();
        return CallbackResponse.DONE;
      }
    }
  }

  class SpannerTailerRunner implements Runnable {
    @Override
    public void run() {
      logger.finer(
          String.format(
              "Starting poll for commit timestamp %s", lastSeenCommitTimestamp.toString()));
      startedPollWithCommitTimestamp = lastSeenCommitTimestamp;
      try (AsyncResultSet rs =
          client
              .singleUse()
              .executeQueryAsync(
                  statement.bind("prevCommitTimestamp").to(lastSeenCommitTimestamp).build())) {
        rs.setCallback(executor, new SpannerTailerCallback(callback));
      }
    }
  }
}
