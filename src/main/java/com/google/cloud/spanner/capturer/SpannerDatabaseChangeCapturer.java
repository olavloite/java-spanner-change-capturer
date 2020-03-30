package com.google.cloud.spanner.capturer;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.capturer.SpannerTableChangeCapturer.RowChangeCallback;
import java.util.Collection;

/** Interface for capturing changes to a Spanner database. */
public interface SpannerDatabaseChangeCapturer {

  /** Returns the {@link DatabaseClient} used for this capturer. */
  DatabaseClient getDatabaseClient();

  /** Returns the names of the tables that is monitored by this capturer. */
  Collection<String> getTables();

  /** Returns the capturer for the given table. */
  SpannerTableChangeCapturer getCapturer(String table);

  /**
   * Run this change capturer and report all changed rows to the given {@link RowChangeCallback}.
   */
  void start(RowChangeCallback callback);

  /** Stop this change capturer. */
  void stop();
}
