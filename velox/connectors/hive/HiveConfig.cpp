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

#include "velox/connectors/hive/HiveConfig.h"

#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::hive {

// static
bool HiveConfig::isImmutablePartitions(const Config* baseConfig) {
  return baseConfig->get<bool>(kImmutablePartitions, true);
}

// static
uint32_t HiveConfig::maxPartitionsPerWriters(const Config* baseConfig) {
  return baseConfig->get<uint32_t>(kMaxPartitionsPerWriters, 100);
}

// static
HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(const Config* baseConfig) {
  auto strBehavior =
      baseConfig->get<std::string>(kInsertExistingPartitionsBehavior);
  if (strBehavior.has_value()) {
    InsertExistingPartitionsBehavior behavior =
        stringToInsertExistingPartitionsBehavior(strBehavior.value());
    if (isImmutablePartitions(baseConfig)) {
      VELOX_CHECK_NE(
          behavior,
          InsertExistingPartitionsBehavior::kAppend,
          "Hive partitions are immutable. {} is not allowed to be set to {}.",
          kInsertExistingPartitionsBehavior,
          strBehavior.value());
    }
    return behavior;
  } else {
    return isImmutablePartitions(baseConfig)
        ? InsertExistingPartitionsBehavior::kError
        : InsertExistingPartitionsBehavior::kAppend;
  }
}

// static
HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::stringToInsertExistingPartitionsBehavior(
    const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "APPEND") {
    return InsertExistingPartitionsBehavior::kAppend;
  }
  if (upperValue == "ERROR") {
    return InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return InsertExistingPartitionsBehavior::kOverwrite;
  }
  VELOX_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace facebook::velox::connector::hive