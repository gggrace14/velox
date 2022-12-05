/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/core/Context.h"

namespace facebook::velox::connector::hive {

/// Hive connector configs
class HiveConfig {
 public:
  enum class InsertExistingPartitionsBehavior {
    kAppend,
    kError,
    kOverwrite,
  };

  /// Can new data be inserted into existing partitions or existing
  /// unpartitioned tables.
  static constexpr const char* kImmutablePartitions = "immutable_partitions";

  /// Maximum number of partitions per writer.
  static constexpr const char* kMaxPartitionsPerWriters =
      "max_partitions_per_writers";

  /// Default value for insert existing partitions behavior.
  static constexpr const char* kInsertExistingPartitionsBehavior =
      "insert_existing_partitions_behavior";

  static bool isImmutablePartitions(const Config* baseConfig);

  static uint32_t maxPartitionsPerWriters(const Config* baseConfig);

  static InsertExistingPartitionsBehavior insertExistingPartitionsBehavior(
      const Config* baseConfig);

 private:
  static InsertExistingPartitionsBehavior
  stringToInsertExistingPartitionsBehavior(const std::string& strValue);
};

} // namespace facebook::velox::connector::hive
