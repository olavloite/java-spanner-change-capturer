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

import com.google.cloud.Timestamp;

/**
 * Interface for storing the last seen commit timestamp by a {@link SpannerTableChangeCapturer} to a
 * persistent repository.
 */
public interface CommitTimestampRepository {

  /** Returns the last seen commit timestamp for the given table. */
  Timestamp get(TableId table);

  /** Sets the last seen commit timestamp for the given table. */
  void set(TableId table, Timestamp commitTimestamp);
}
