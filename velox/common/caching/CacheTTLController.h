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

#include "velox/common/time/Timer.h"

#include "folly/Synchronized.h"
#include "folly/container/F14Map.h"

namespace facebook::velox::cache {

class AsyncDataCache;

struct RawFileInfo {
  int64_t openTimeSec;

  bool operator==(const RawFileInfo& other) {
    return openTimeSec == other.openTimeSec;
  }
};

struct CacheAgeStats {
  int64_t maxAgeSecs{0};
};

class CacheTTLController {
 public:
  static CacheTTLController* create(AsyncDataCache& cache) {
    if (instance_ == nullptr) {
      instance_ =
          std::unique_ptr<CacheTTLController>(new CacheTTLController(cache));
    }
    return instance_.get();
  }

  static CacheTTLController* getInstance() {
    if (instance_ == nullptr) {
      return nullptr;
    }
    return instance_.get();
  }

  /// Add file opening info for fileNum and return true if fileNum is not in the
  /// map. If the map already includes fileNum, no action will happen and return
  /// false.
  bool addOpenFileInfo(
      uint64_t fileNum,
      int64_t openTimeSec = getCurrentTimeSec());

  CacheAgeStats getCacheAgeStats() const;

 private:
  static std::unique_ptr<CacheTTLController> instance_;

 private:
  explicit CacheTTLController(AsyncDataCache& cache) : cache_(cache) {}

  AsyncDataCache& cache_;
  folly::Synchronized<folly::F14FastMap<uint64_t, RawFileInfo>> fileInfoMap_;
};

} // namespace facebook::velox::cache