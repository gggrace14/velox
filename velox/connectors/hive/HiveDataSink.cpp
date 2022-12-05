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

#include "velox/connectors/hive/HiveDataSink.h"

#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HivePartitionUtil.h"
#include "velox/connectors/hive/HiveWriteProtocol.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/OperatorUtils.h"

using namespace facebook::velox::dwrf;
using WriterConfig = facebook::velox::dwrf::Config;

namespace facebook::velox::connector::hive {

namespace {
std::vector<column_index_t> getPartitionChannels(
    const std::shared_ptr<const HiveInsertTableHandle>& insertTableHandle) {
  std::vector<column_index_t> channels;

  for (column_index_t i = 0; i < insertTableHandle->inputColumns().size();
       i++) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }

  return channels;
}

// Compute the count of rows as well as the concrete row indices for every
// partition ID based on the ID labeling of rowPartitionIds. Both rowCounts
// and rowIndices are indexed by partition ID. rowIndices[i] is a vector of
// all rows labeled with partition ID i.
void computePartitionRowCountsAndIndices(
    const raw_vector<uint64_t>& partitionIds,
    uint64_t numPartitionIds,
    memory::MemoryPool* FOLLY_NONNULL pool,
    std::vector<vector_size_t>& partitionSizes,
    std::vector<BufferPtr>& partitionRows) {
  partitionSizes.resize(numPartitionIds);
  std::fill(partitionSizes.begin(), partitionSizes.end(), 0);
  partitionRows.resize(numPartitionIds);
  std::fill(partitionRows.begin(), partitionRows.end(), nullptr);

  std::vector<vector_size_t*> rawIndexValues(numPartitionIds);
  uint64_t numRows = partitionIds.size();

  for (vector_size_t row = 0; row < numRows; row++) {
    uint64_t id = partitionIds[row];

    if (partitionRows[id] == nullptr) {
      partitionRows[id] = allocateIndices(numRows, pool);
      rawIndexValues[id] = partitionRows[id]->asMutable<vector_size_t>();
    }

    rawIndexValues[id][partitionSizes[id]] = row;
    partitionSizes[id]++;
  }

  for (auto id = 0; id < numPartitionIds; id++) {
    if (partitionRows[id] != nullptr) {
      partitionRows[id]->setSize(partitionSizes[id] * sizeof(vector_size_t));
    }
  }
}

} // namespace

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
    std::shared_ptr<WriteProtocol> writeProtocol)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      writeProtocol_(std::move(writeProtocol)),
      partitionChannels_(getPartitionChannels(insertTableHandle_)),
      partitionIdGenerator_(
          inputType_,
          partitionChannels_,
          connectorQueryCtx_->memoryPool()),
      numPartitions_(0) {
  VELOX_CHECK_NOT_NULL(
      writeProtocol_, "Write protocol could not be nullptr for HiveDataSink.");
}

std::shared_ptr<ConnectorCommitInfo> HiveDataSink::getConnectorCommitInfo()
    const {
  std::vector<std::shared_ptr<const HiveWriterParameters>> toCommit;
  toCommit.reserve(numPartitions_);

  for (auto i = 0; i < writerParameters_.size(); i++) {
    if (writerParameters_[i] != nullptr) {
      toCommit.push_back(writerParameters_[i]);
    }
  }
  return std::make_shared<HiveConnectorCommitInfo>(toCommit);
}

void HiveDataSink::appendData(VectorPtr input) {
  input_ = std::dynamic_pointer_cast<RowVector>(input);
  auto numRows = input->size();

  // Writing to unpartitioned table
  if (partitionChannels_.size() == 0) {
    if (writers_.empty()) {
      createWriterForPartition(0);
    }
    writers_[0]->write(input_);
    return;
  }

  // Writing to partitioned table
  std::map<uint64_t, uint64_t> idMap =
      partitionIdGenerator_.run(input_, partitionIds_);

  // If the current input causes the partitionIdGenerator_ to rehash which
  // changes the IDs of the previous partition values, move the existing writers
  // for the previous partition values to new indices.
  if (!idMap.empty() && !writers_.empty()) {
    moveWriters(idMap);
  }

  uint64_t maxPartitionId = partitionIdGenerator_.getMaxPartitionId();

  computePartitionRowCountsAndIndices(
      partitionIds_,
      maxPartitionId + 1,
      connectorQueryCtx_->memoryPool(),
      partitionSizes_,
      partitionRows_);

  for (column_index_t i = 0; i < input_->childrenSize(); i++) {
    input_->childAt(i)->loadedVector();
  }

  for (vector_size_t partitionId = 0; partitionId <= maxPartitionId;
       partitionId++) {
    if (partitionSizes_[partitionId] == 0) {
      continue;
    }

    if (writers_.size() < (partitionId + 1) || !writers_[partitionId]) {
      numPartitions_++;
      VELOX_USER_CHECK_LE(
          numPartitions_,
          HiveConfig::maxPartitionsPerWriters(connectorQueryCtx_->config()),
          "Exceeded limit of open writers for partitions",
          HiveConfig::maxPartitionsPerWriters(connectorQueryCtx_->config()));

      createWriterForPartition(partitionId);
    }

    RowVectorPtr writerInput = exec::wrap(
        partitionSizes_[partitionId], partitionRows_[partitionId], input_);
    writers_[partitionId]->write(writerInput);
  }
}

void HiveDataSink::close() {
  for (const auto& writer : writers_) {
    if (writer) {
      writer->close();
    }
  }
}

std::unique_ptr<velox::dwrf::Writer> HiveDataSink::createWriter(
    const std::shared_ptr<const HiveWriterParameters>& writerParameters) const {
  auto config = std::make_shared<WriterConfig>();
  // TODO: Wire up serde properties to writer configs.

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = inputType_;
  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.

  auto writePath = fs::path(writerParameters->writeDirectory()) /
      writerParameters->writeFileName();
  auto sink = dwio::common::DataSink::create(writePath);
  return std::make_unique<Writer>(
      options, std::move(sink), *connectorQueryCtx_->memoryPool());
}

void HiveDataSink::createWriterForPartition(vector_size_t partitionId) {
  if (writers_.size() < (partitionId + 1)) {
    writers_.resize(partitionId + 1);
    writerParameters_.resize(partitionId + 1);
  }

  std::optional<std::string> partitionName = partitionChannels_.size() > 0
      ? std::optional<std::string>(makePartitionName(
            input_,
            partitionChannels_,
            partitionRows_[partitionId]->as<vector_size_t>()[0]))
      : std::nullopt;
  auto writerParameters = std::dynamic_pointer_cast<const HiveWriterParameters>(
      writeProtocol_->getWriterParameters(
          insertTableHandle_,
          connectorQueryCtx_,
          std::make_shared<HiveConnectorWriteInfo>(partitionName)));
  VELOX_CHECK_NOT_NULL(
      writerParameters, "Hive data sink expects write parameters for Hive.");
  writerParameters_[partitionId] = writerParameters;

  writers_[partitionId] = createWriter(writerParameters);
}

void HiveDataSink::moveWriters(const std::map<uint64_t, uint64_t>& idMap) {
  uint64_t maxNewIndex = idMap.rbegin()->second;

  if (writers_.size() < maxNewIndex + 1) {
    writers_.resize(maxNewIndex + 1);
    writerParameters_.resize(maxNewIndex + 1);
  }
  // Rehash monotonically makes the IDs of previous partition values larger. So
  // moving the existing writers to the high indices starting with the high
  // end is safe and will not overwrite existing ones.
  for (auto entry = idMap.rbegin(); entry != idMap.rend(); entry++) {
    if (entry->first == entry->second) {
      continue;
    }
    VELOX_CHECK_NOT_NULL(
        writers_[entry->first],
        "Found nullptr for the writer at index {}.",
        entry->first);

    writers_[entry->second] = std::move(writers_[entry->first]);
    writers_[entry->first] = nullptr;

    writerParameters_[entry->second] =
        std::move(writerParameters_[entry->first]);
    writerParameters_[entry->first] = nullptr;
  }
}

bool HiveInsertTableHandle::isPartitioned() const {
  return std::any_of(
      inputColumns_.begin(), inputColumns_.end(), [](auto column) {
        return column->isPartitionKey();
      });
}

bool HiveInsertTableHandle::isCreateTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kNew;
}

bool HiveInsertTableHandle::isInsertTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
}

} // namespace facebook::velox::connector::hive
