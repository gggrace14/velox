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
#include "velox/connectors/hive/HiveConnector.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace facebook::velox::connector::hive {

namespace {

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

} // namespace

std::shared_ptr<const WriterParameters>
HiveNoCommitWriteProtocol::getWriterParameters(
    const std::shared_ptr<const ConnectorInsertTableHandle>& tableWriteHandle,
    const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
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

  VELOX_USER_CHECK(
      hiveTableWriteHandle->isCreateTable() ||
          !HiveConfig::isImmutablePartitions(connectorQueryCtx->config()),
      "Unpartitioned Hive tables are immutable");

  auto targetFileName = fmt::format(
      "{}_{}_{}",
      connectorQueryCtx->taskId(),
      connectorQueryCtx->driverId(),
      makeUuid());
  auto targetDirectory = hiveWriteInfo->partitionDirectory().has_value()
      ? fmt::format(
            "{}/{}",
            hiveTableWriteHandle->locationHandle()->targetPath(),
            hiveWriteInfo->partitionDirectory().value())
      : hiveTableWriteHandle->locationHandle()->targetPath();
  auto writeDirectory = hiveWriteInfo->partitionDirectory().has_value()
      ? fmt::format(
            "{}/{}",
            hiveTableWriteHandle->locationHandle()->writePath(),
            hiveWriteInfo->partitionDirectory().value())
      : hiveTableWriteHandle->locationHandle()->writePath();
  //  auto targetDirectory =
  //  hiveTableWriteHandle->locationHandle()->targetPath(); auto writeDirectory
  //  = hiveTableWriteHandle->locationHandle()->writePath();

  return std::make_shared<HiveWriterParameters>(
      hiveTableWriteHandle->isCreateTable()
          ? HiveWriterParameters::UpdateMode::kNew
          : HiveWriterParameters::UpdateMode::kAppend,
      targetFileName,
      targetDirectory,
      targetFileName,
      writeDirectory);
}

std::shared_ptr<const WriterParameters>
HiveTaskCommitWriteProtocol::getWriterParameters(
    const std::shared_ptr<const ConnectorInsertTableHandle>& tableWriteHandle,
    const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
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

  VELOX_USER_CHECK(
      hiveTableWriteHandle->isCreateTable() ||
          !HiveConfig::isImmutablePartitions(connectorQueryCtx->config()),
      "Unpartitioned Hive tables are immutable");

  auto targetFileName = fmt::format(
      "{}_{}_{}",
      connectorQueryCtx->taskId(),
      connectorQueryCtx->driverId(),
      0);
  auto writeFileName =
      fmt::format(".tmp.velox.{}_{}", targetFileName, makeUuid());
  auto targetDirectory = hiveWriteInfo->partitionDirectory().has_value()
      ? fmt::format(
            "{}/{}",
            hiveTableWriteHandle->locationHandle()->targetPath(),
            hiveWriteInfo->partitionDirectory().value())
      : hiveTableWriteHandle->locationHandle()->targetPath();
  auto writeDirectory = hiveWriteInfo->partitionDirectory().has_value()
      ? fmt::format(
            "{}/{}",
            hiveTableWriteHandle->locationHandle()->writePath(),
            hiveWriteInfo->partitionDirectory().value())
      : hiveTableWriteHandle->locationHandle()->writePath();

  return std::make_shared<HiveWriterParameters>(
      hiveTableWriteHandle->isCreateTable()
          ? HiveWriterParameters::UpdateMode::kNew
          : HiveWriterParameters::UpdateMode::kAppend,
      targetFileName,
      targetDirectory,
      writeFileName,
      writeDirectory);
}

} // namespace facebook::velox::connector::hive
