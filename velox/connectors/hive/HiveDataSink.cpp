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
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveWriteProtocol.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox::dwrf;
using WriterConfig = facebook::velox::dwrf::Config;

namespace facebook::velox::connector::hive {

namespace {
std::vector<column_index_t> getPartitionChannels(
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle) {
  std::vector<column_index_t> channels;

  for (column_index_t i = 0; i < insertTableHandle->inputColumns().size();
       i++) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }

  return std::move(channels);
}

RowTypePtr getChannelTypes(
    RowTypePtr rowType,
    const std::vector<column_index_t>& channels) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(channels.size());
  types.reserve(channels.size());

  for (column_index_t index : channels) {
    names.push_back(rowType->nameOf(index));
    types.push_back(rowType->childAt(index));
  }

  return ROW(std::move(names), std::move(types));
}

uint64_t getMaxPartitionId(
    const raw_vector<uint64_t>& partitionIds,
    const SelectivityVector& activeRows) {
  uint64_t result = 0;
  activeRows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    result = std::max(result, partitionIds[row]);
  });
  return result;
}

// TODO(gaoge): escape path characters as in
// https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/FileUtils.java
template <typename T>
inline std::string makeColumnString(T value) {
  return folly::to<std::string>(value);
}

template <>
inline std::string makeColumnString(Date value) {
  return value.toString();
}

template <TypeKind Kind>
std::string makePartitionColumnString(
    const VectorPtr& partitionVector,
    vector_size_t row,
    const std::string& name) {
  using T = typename KindToFlatVector<Kind>::WrapperType;
  return fmt::format(
      "{}={}",
      name,
      makeColumnString(
          partitionVector->asUnchecked<FlatVector<T>>()->valueAt(row)));
};

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
      partitionsType_(getChannelTypes(inputType_, partitionChannels_)),
      partitionIdGenerator_(
          partitionsType_,
          partitionChannels_,
          connectorQueryCtx_->config()) {
  VELOX_CHECK_NOT_NULL(
      writeProtocol_, "Write protocol could not be nullptr for HiveDataSink.");

  partitionsVector_ = std::static_pointer_cast<RowVector>(
      BaseVector::create(partitionsType_, 0, connectorQueryCtx_->memoryPool()));
}

std::shared_ptr<ConnectorCommitInfo> HiveDataSink::getConnectorCommitInfo()
    const {
  std::vector<std::string> partitionNames;
  partitionNames.reserve(writers_.size());
  std::vector<std::shared_ptr<const HiveWriterParameters>> writerParameters;
  writerParameters.reserve(writers_.size());

  for (auto i = 0; i < writers_.size(); i++) {
    if (writers_[i] != nullptr) {
      partitionNames.push_back(getPartitionString(partitionsVector_, i));
      writerParameters.push_back(
          std::static_pointer_cast<const HiveWriterParameters>(
              writeProtocol_->getWriterParameters(
                  insertTableHandle_,
                  connectorQueryCtx_,
                  std::make_shared<HiveConnectorWriteInfo>(
                      partitionNames.back()))));
    }
  }
  return std::make_shared<HiveConnectorCommitInfo>(
      writerParameters, partitionNames);
}

void HiveDataSink::appendData(VectorPtr input) {
  input_ = std::dynamic_pointer_cast<RowVector>(input);

  auto numRows = input->size();
  auto numPartitions = partitionChannels_.size();

  if (numPartitions == 0) {
    if (writers_.empty()) {
      auto writerParameters =
          std::dynamic_pointer_cast<const HiveWriterParameters>(
              writeProtocol_->getWriterParameters(
                  insertTableHandle_,
                  connectorQueryCtx_,
                  std::make_shared<HiveConnectorWriteInfo>(std::nullopt)));
      VELOX_CHECK_NOT_NULL(
          writerParameters,
          "Hive data sink expects write parameters for Hive.");
      writers_.push_back(createWriter(writerParameters));
    }

    writers_[0]->write(input_);
    return;
  }

  activeRows_.resize(numRows);
  activeRows_.setAll();
  bool stable = partitionIdGenerator_.run(input_, activeRows_, partitionIds_);

  if (!stable && !writers_.empty()) {
    moveWriters();
  }

  uint64_t maxPartitionId = getMaxPartitionId(partitionIds_, activeRows_);

  std::vector<BufferPtr> partitionRowIndices;
  computePartitionRowCountsAndIndices(
      partitionIds_,
      activeRows_,
      maxPartitionId + 1,
      connectorQueryCtx_->memoryPool(),
      partitionRowCounts_,
      partitionRowIndices);

  if (writers_.size() < maxPartitionId + 1) {
    partitionsVector_->resize(maxPartitionId + 1);
    writers_.resize(maxPartitionId + 1);
  }

  std::vector<BaseVector::CopyRange> copyRanges;
  for (vector_size_t partitionId = 0; partitionId <= maxPartitionId;
       partitionId++) {
    if (partitionRowCounts_[partitionId] > 0 && !writers_[partitionId]) {
      copyRanges.push_back(
          {.sourceIndex =
               partitionRowIndices[partitionId]->as<vector_size_t>()[0],
           .targetIndex = partitionId,
           .count = 1});
    }
  }
  partitionsVector_->copyRanges(input_.get(), copyRanges);

  for (vector_size_t partitionId = 0; partitionId <= maxPartitionId;
       partitionId++) {
    if (partitionRowCounts_[partitionId] == 0) {
      continue;
    }
    if (!writers_[partitionId]) {
      std::string partitionString =
          getPartitionString(partitionsVector_, partitionId);

      auto writerParameters =
          std::dynamic_pointer_cast<const HiveWriterParameters>(
              writeProtocol_->getWriterParameters(
                  insertTableHandle_,
                  connectorQueryCtx_,
                  std::make_shared<HiveConnectorWriteInfo>(partitionString)));
      VELOX_CHECK_NOT_NULL(
          writerParameters,
          "Hive data sink expects write parameters for Hive.");
      writers_[partitionId] = createWriter(writerParameters);
    }

    RowVectorPtr writerInput = makeWriterInput(
        input_,
        partitionRowCounts_[partitionId],
        partitionRowIndices[partitionId]);
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
    const std::shared_ptr<const HiveWriterParameters>& writerParameters) {
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

void HiveDataSink::moveWriters() {
  raw_vector<uint64_t> newIndices;

  SelectivityVector validRows = SelectivityVector::empty(writers_.size());
  for (vector_size_t i = 0; i < writers_.size(); i++) {
    if (writers_[i]) {
      validRows.setValid(i, true);
    }
  }
  validRows.updateBounds();

  partitionIdGenerator_.rerun(partitionsVector_, validRows, newIndices);

  uint64_t maxNewIndex = getMaxPartitionId(newIndices, validRows);

  if (writers_.size() < maxNewIndex + 1) {
    writers_.resize(maxNewIndex + 1);
    partitionsVector_->resize(maxNewIndex + 1, false);
  }
  for (vector_size_t i = writers_.size() - 1; i >= 0; i--) {
    if (writers_[i] && i != newIndices[i]) {
      writers_[newIndices[i]] = std::move(writers_[i]);
      writers_[i] = nullptr;

      partitionsVector_->copy(partitionsVector_.get(), newIndices[i], i, 1);
      partitionsVector_->setNull(i, true);
    }
  }
}

// static
RowVectorPtr HiveDataSink::makeWriterInput(
    const RowVectorPtr& rawInput,
    vector_size_t size,
    const BufferPtr& index) {
  const auto numChildren = rawInput->childrenSize();

  std::vector<VectorPtr> dictionaryChildren;
  dictionaryChildren.reserve(numChildren);

  for (auto i = 0; i < numChildren; i++) {
    dictionaryChildren.push_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), index, size, rawInput->childAt(i)));
  }

  return std::make_shared<RowVector>(
      rawInput->pool(),
      rawInput->type(),
      BufferPtr(nullptr),
      size,
      dictionaryChildren);
}

// static
void HiveDataSink::computePartitionRowCountsAndIndices(
    const raw_vector<uint64_t>& rowPartitionIds,
    const SelectivityVector& activeRows,
    uint64_t numPartitionIds,
    memory::MemoryPool* FOLLY_NONNULL pool,
    std::vector<vector_size_t>& rowCounts,
    std::vector<BufferPtr>& rowIndices) {
  rowCounts.resize(numPartitionIds);
  std::fill(rowCounts.begin(), rowCounts.end(), 0);
  rowIndices.resize(numPartitionIds);
  std::fill(rowIndices.begin(), rowIndices.end(), nullptr);

  std::vector<vector_size_t*> rawIndexValues(numPartitionIds);
  uint64_t numRows = rowPartitionIds.size();

  activeRows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    uint64_t id = rowPartitionIds[row];

    if (rowIndices.at(id) == nullptr) {
      rowIndices[id] = allocateIndices(numRows, pool);
      rawIndexValues[id] = rowIndices[id]->asMutable<vector_size_t>();
    }

    rawIndexValues[id][rowCounts[id]] = row;
    rowCounts[id]++;
  });

  for (auto id = 0; id < numPartitionIds; id++) {
    if (rowIndices[id] != nullptr) {
      rowIndices[id]->setSize(rowCounts[id] * sizeof(vector_size_t));
    }
  }
}

// static
std::string HiveDataSink::getPartitionString(
    const RowVectorPtr& partitionsVector,
    vector_size_t row) {
  std::stringstream ss;
  for (column_index_t i = 0; i < partitionsVector->childrenSize(); i++) {
    if (i > 0) {
      ss << '/';
    }
    ss << PARTITION_TYPE_DISPATCH(
        makePartitionColumnString,
        partitionsVector->childAt(i)->typeKind(),
        partitionsVector->childAt(i),
        row,
        asRowType(partitionsVector->type())->nameOf(i));
  }
  return ss.str();
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

bool PartitionIdGenerator::run(
    RowVectorPtr input,
    const SelectivityVector& activeRows,
    raw_vector<uint64_t>& result) {
  return runInternal(input, activeRows, true, result);
}

void PartitionIdGenerator::rerun(
    RowVectorPtr input,
    const SelectivityVector& activeRows,
    raw_vector<uint64_t>& result) {
  runInternal(input, activeRows, false, result);
}

bool PartitionIdGenerator::runInternal(
    RowVectorPtr input,
    const SelectivityVector& activeRows,
    bool allowRehash,
    raw_vector<uint64_t>& result) {
  result.resize(input->size());

  for (auto i = 0; i < hashers_.size(); i++) {
    auto partitionVector =
        input->childAt(hashers_[i]->channel())->loadedVector();
    hashers_[i]->decode(*partitionVector, activeRows);
  }

  bool stable = true;
  bool rehash;
  do {
    rehash = false;
    for (auto i = 0; i < hashers_.size(); i++) {
      if (!hashers_[i]->computeValueIds(activeRows, result)) {
        rehash = true;
      }
    }
    if (rehash) {
      VELOX_CHECK(
          allowRehash,
          "The input changes the internal structure of PartitionIdGenerator and causes it to rehash that is not allowed.");
      stable = false;

      uint64_t multiplier = 1;
      for (auto i = 0; i < hashers_.size(); i++) {
        multiplier = hashers_[i]->enableValueIds(multiplier, kHasherReservePct);
        //        VELOX_CHECK(
        //            multiplier != exec::VectorHasher::kRangeTooLarge &&
        //                multiplier <=
        //                    HiveConfig::maxPartitionsPerWriters(connectorConfig_),
        //            "Exceeded limit of {} open writers for
        //            partitions/buckets.",
        //            HiveConfig::maxPartitionsPerWriters(connectorConfig_));
      }
    }
  } while (rehash);

  return stable;
}

} // namespace facebook::velox::connector::hive
