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

#include "velox/connectors/hive/HiveWriteProtocol.h"

#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace facebook::velox::connector::hive {

namespace {

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return (fs::path(tableDirectory) / partitionSubdirectory.value()).string();
  }
  return tableDirectory;
}

HiveWriterParameters::UpdateMode validateAndGetUpdateMode(
    const std::shared_ptr<const HiveInsertTableHandle>& hiveTableWriteHandle,
    const ConnectorQueryCtx* connectorQueryCtx) {
  if (hiveTableWriteHandle->isInsertTable()) {
    if (hiveTableWriteHandle->isPartitioned()) {
      HiveConfig::InsertExistingPartitionsBehavior insertBehavior =
          HiveConfig::insertExistingPartitionsBehavior(
              connectorQueryCtx->config());

      switch (insertBehavior) {
        case HiveConfig::InsertExistingPartitionsBehavior::kOverwrite:
          return HiveWriterParameters::UpdateMode::kOverwrite;

        case HiveConfig::InsertExistingPartitionsBehavior::kError:
          return HiveWriterParameters::UpdateMode::kNew;

          // TODO(gaoge): Include pageSinkMetadata and modifiedPartitions
          // fields in HiveInsertTableHandle to support
          // hive.fail-fast-on-insert-into-immutable-partitions-enabled.
        case HiveConfig::InsertExistingPartitionsBehavior::kAppend:
          VELOX_UNSUPPORTED("Insert into existing partition is not supported.");

          // TODO(gaoge): Include pageSinkMetadata and modifiedPartitions
          // fields in HiveInsertTableHandle to support insert into existing
          // partition with APPEND behavior.
        default:
          VELOX_UNSUPPORTED("Unsupported insert existing partitions behavior.");
      }
    } else {
      VELOX_USER_CHECK(
          !HiveConfig::isImmutablePartitions(connectorQueryCtx->config()),
          "Unpartitioned Hive tables are immutable.");

      return HiveWriterParameters::UpdateMode::kAppend;
    }
  } else {
    return HiveWriterParameters::UpdateMode::kNew;
  }
}

} // namespace

std::shared_ptr<const WriterParameters>
HiveNoCommitWriteProtocol::getWriterParameters(
    const std::shared_ptr<const ConnectorInsertTableHandle>& tableWriteHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const ConnectorWriteInfo>& writeInfo) const {
  auto hiveTableWriteHandle =
      std::dynamic_pointer_cast<const HiveInsertTableHandle>(tableWriteHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableWriteHandle,
      "This write protocol cannot be used for non-Hive connector");
  auto hiveWriteInfo =
      std::dynamic_pointer_cast<const HiveConnectorWriteInfo>(writeInfo);
  VELOX_CHECK_NOT_NULL(
      hiveWriteInfo,
      "This write protocol cannot be used for non-Hive connector");

  auto updateMode =
      validateAndGetUpdateMode(hiveTableWriteHandle, connectorQueryCtx);
  auto targetFileName = fmt::format(
      "{}_{}_{}",
      connectorQueryCtx->taskId(),
      connectorQueryCtx->driverId(),
      makeUuid());
  auto targetDirectory = makePartitionDirectory(
      hiveTableWriteHandle->locationHandle()->targetPath(),
      hiveWriteInfo->partitionDirectory());
  auto writeDirectory = makePartitionDirectory(
      hiveTableWriteHandle->locationHandle()->writePath(),
      hiveWriteInfo->partitionDirectory());

  return std::make_shared<HiveWriterParameters>(
      updateMode,
      hiveWriteInfo->partitionDirectory(),
      targetFileName,
      targetDirectory,
      targetFileName,
      writeDirectory);
}

std::shared_ptr<const WriterParameters>
HiveTaskCommitWriteProtocol::getWriterParameters(
    const std::shared_ptr<const ConnectorInsertTableHandle>& tableWriteHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const ConnectorWriteInfo>& writeInfo) const {
  auto hiveTableWriteHandle =
      std::dynamic_pointer_cast<const HiveInsertTableHandle>(tableWriteHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableWriteHandle,
      "This write protocol cannot be used for non-Hive connector");
  auto hiveWriteInfo =
      std::dynamic_pointer_cast<const HiveConnectorWriteInfo>(writeInfo);
  VELOX_CHECK_NOT_NULL(
      hiveWriteInfo,
      "This write protocol cannot be used for non-Hive connector");

  auto updateMode =
      validateAndGetUpdateMode(hiveTableWriteHandle, connectorQueryCtx);
  auto targetFileName = fmt::format(
      "{}_{}_{}",
      connectorQueryCtx->taskId(),
      connectorQueryCtx->driverId(),
      0);
  auto writeFileName =
      fmt::format(".tmp.velox.{}_{}", targetFileName, makeUuid());
  auto targetDirectory = makePartitionDirectory(
      hiveTableWriteHandle->locationHandle()->targetPath(),
      hiveWriteInfo->partitionDirectory());
  auto writeDirectory = makePartitionDirectory(
      hiveTableWriteHandle->locationHandle()->writePath(),
      hiveWriteInfo->partitionDirectory());

  return std::make_shared<HiveWriterParameters>(
      updateMode,
      hiveWriteInfo->partitionDirectory(),
      targetFileName,
      targetDirectory,
      writeFileName,
      writeDirectory);
}

} // namespace facebook::velox::connector::hive
