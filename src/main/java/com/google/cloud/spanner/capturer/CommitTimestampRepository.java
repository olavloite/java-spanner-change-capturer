package com.google.cloud.spanner.capturer;

import com.google.cloud.Timestamp;

/** Interface for storing the last seen commit timestamp to a persistent repository. */
public interface CommitTimestampRepository {

  /** Returns the last seen commit timestamp for the given table name. */
  Timestamp get(String table);

  /** Sets the last seen commit timestamp for the given table name. */
  void set(String table, Timestamp commitTimestamp);
}
