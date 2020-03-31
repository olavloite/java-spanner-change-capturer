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

import com.google.api.core.ApiFuture;
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
  ApiFuture<Void> stopAsync();
}
