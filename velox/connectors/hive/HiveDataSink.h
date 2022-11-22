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

#include "velox/connectors/Connector.h"
#include "velox/exec/VectorHasher.h"

#include <algorithm>

namespace facebook::velox::dwrf {
class Writer;
}

namespace facebook::velox::connector::hive {
class HiveColumnHandle;
class HiveWriterParameters;

/// Location related properties of the Hive table to be written
class LocationHandle {
 public:
  enum class TableType {
    kNew, // Write to a new table to be created.
    kExisting, // Write to an existing table.
    kTemporary, // Write to a temporary table.
  };

  enum class WriteMode {
    // Write to a staging directory and then move to the target directory
    // after write finishes.
    kStageAndMoveToTargetDirectory,
    // Directly write to the target directory to be created.
    kDirectToTargetNewDirectory,
    // Directly write to the existing target directory.
    kDirectToTargetExistingDirectory,
  };

  LocationHandle(
      std::string targetPath,
      std::string writePath,
      TableType tableType,
      WriteMode writeMode)
      : targetPath_(std::move(targetPath)),
        writePath_(std::move(writePath)),
        tableType_(tableType),
        writeMode_(writeMode) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& writePath() const {
    return writePath_;
  }

  TableType tableType() const {
    return tableType_;
  }

  WriteMode writeMode() const {
    return writeMode_;
  }

 private:
  // Target directory path.
  const std::string targetPath_;
  // Staging directory path.
  const std::string writePath_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
  // How the target path and directory path could be used.
  const WriteMode writeMode_;
};

/**
 * Represents a request for Hive write
 */
class HiveInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  HiveInsertTableHandle(
      std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns,
      std::shared_ptr<const LocationHandle> locationHandle)
      : inputColumns_(std::move(inputColumns)),
        locationHandle_(std::move(locationHandle)) {}

  virtual ~HiveInsertTableHandle() = default;

  const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns()
      const {
    return inputColumns_;
  }

  const std::shared_ptr<const LocationHandle>& locationHandle() const {
    return locationHandle_;
  }

  bool isPartitioned() const;

  bool isCreateTable() const;

  bool isInsertTable() const;

 private:
  const std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;
};


class PartitionIdGenerator {
 public:
  static constexpr const int32_t kHasherReservePct = 20;

  explicit PartitionIdGenerator(
      RowTypePtr partitionType,
      std::vector<column_index_t> partitionChannels,
      const Config* FOLLY_NONNULL connectorConfig)
      : partitionType_(std::move(partitionType)),
        connectorConfig_(std::move(connectorConfig)) {
    hashers_.reserve(partitionType_->size());
    for (auto i = 0; i < partitionType_->size(); i++) {
      hashers_.push_back(exec::VectorHasher::create(
          partitionType_->childAt(i), partitionChannels[i]));
    }
  }

  bool run(
      RowVectorPtr input,
      const SelectivityVector& activeRows,
      raw_vector<uint64_t>& result);

  void rerun(
      RowVectorPtr input,
      const SelectivityVector& activeRows,
      raw_vector<uint64_t>& result);

 private:
  bool runInternal(
      RowVectorPtr input,
      const SelectivityVector& activeRows,
      bool allowRehash,
      raw_vector<uint64_t>& result);

  const RowTypePtr partitionType_;
  const Config* FOLLY_NONNULL connectorConfig_;

  std::vector<std::unique_ptr<exec::VectorHasher>> hashers_;
};

class HiveDataSink : public DataSink {
 public:
  explicit HiveDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
      std::shared_ptr<WriteProtocol> writeProtocol);

  std::shared_ptr<ConnectorCommitInfo> getConnectorCommitInfo() const override;

  void appendData(VectorPtr input) override;

  void close() override;

 private:
  std::unique_ptr<dwrf::Writer> createWriter(
      const std::shared_ptr<const HiveWriterParameters>& writerParameters);

  void moveWriters();

  static RowVectorPtr makeWriterInput(
      const RowVectorPtr& rawInput,
      vector_size_t size,
      const BufferPtr& index);

  static void computePartitionRowCountsAndIndices(
      const raw_vector<uint64_t>& rowPartitionIds,
      const SelectivityVector& activeRows,
      uint64_t maxPartitionId,
      memory::MemoryPool* FOLLY_NONNULL pool,
      std::vector<vector_size_t>& rowCounts,
      std::vector<BufferPtr>& rowIndices);

  static std::string getPartitionString(
      const RowVectorPtr& partitionsString,
      vector_size_t row);

  const RowTypePtr inputType_;
  const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx_;
  const std::shared_ptr<WriteProtocol> writeProtocol_;
  const std::vector<column_index_t> partitionChannels_;
  const RowTypePtr partitionsType_;
  PartitionIdGenerator partitionIdGenerator_;

  std::vector<std::unique_ptr<dwrf::Writer>> writers_;
  RowVectorPtr partitionsVector_;
  std::vector<vector_size_t> partitionRowCounts_;

  RowVectorPtr input_;
  SelectivityVector activeRows_;
  raw_vector<uint64_t> partitionIds_;
};

#define PARTITION_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...)             \
  [&]() {                                                                 \
    switch (typeKind) {                                                   \
      case TypeKind::BOOLEAN: {                                           \
        return TEMPLATE_FUNC<TypeKind::BOOLEAN>(__VA_ARGS__);             \
      }                                                                   \
      case TypeKind::TINYINT: {                                           \
        return TEMPLATE_FUNC<TypeKind::TINYINT>(__VA_ARGS__);             \
      }                                                                   \
      case TypeKind::SMALLINT: {                                          \
        return TEMPLATE_FUNC<TypeKind::SMALLINT>(__VA_ARGS__);            \
      }                                                                   \
      case TypeKind::INTEGER: {                                           \
        return TEMPLATE_FUNC<TypeKind::INTEGER>(__VA_ARGS__);             \
      }                                                                   \
      case TypeKind::BIGINT: {                                            \
        return TEMPLATE_FUNC<TypeKind::BIGINT>(__VA_ARGS__);              \
      }                                                                   \
      case TypeKind::VARCHAR:                                             \
      case TypeKind::VARBINARY: {                                         \
        return TEMPLATE_FUNC<TypeKind::VARCHAR>(__VA_ARGS__);             \
      }                                                                   \
      case TypeKind::DATE: {                                              \
        return TEMPLATE_FUNC<TypeKind::DATE>(__VA_ARGS__);                \
      }                                                                   \
      default:                                                            \
        VELOX_UNREACHABLE(                                                \
            "Unsupported partition type: ", mapTypeKindToName(typeKind)); \
    }                                                                     \
  }()

} // namespace facebook::velox::connector::hive
