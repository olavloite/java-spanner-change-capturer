package com.google.cloud.spanner.capturer;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;

/** Interface for capturing changes to a Spanner table. */
public interface SpannerTableChangeCapturer {

  /** Returns the {@link DatabaseClient} used for this capturer. */
  public DatabaseClient getDatabaseClient();

  /** Returns the name of the table that is monitored by this capturer. */
  public String getTable();

  /** Row is passed in to the change callback and allows access to the most recent data. */
  public static interface Row extends StructReader {
    /** Convert the row to a {@link Struct}. */
    Struct asStruct();
  }

  /** Interface for receiving asynchronous callbacks when a row has been inserted or updated. */
  public static interface RowChangeCallback {
    /** Called once for each detected insert or update of a row. */
    void rowChange(String table, Row row);
  }

  /**
   * Run this Change Capturer and report all changed rows to the given {@link RowChangeCallback}.
   */
  void start(RowChangeCallback callback);

  /** Stop this Change Capturer. */
  void stop();
}
