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

#include "velox/common/caching/CacheTTLController.h"

namespace facebook::velox::cache {

bool CacheTTLController::addOpenFileInfo(
    uint64_t fileNum,
    int64_t openTimeSec) {
  auto lockedFileMap = fileInfoMap_.wlock();
  return lockedFileMap->emplace(fileNum, RawFileInfo{openTimeSec}).second;
}

CacheAgeStats CacheTTLController::getCacheAgeStats() const {
  auto lockedFileMap = fileInfoMap_.rlock();

  if (lockedFileMap->empty()) {
    return CacheAgeStats{.maxAgeSecs = 0};
  }

  int64_t minOpenTime = std::numeric_limits<int64_t>::max();

  for (auto it = lockedFileMap->cbegin(); it != lockedFileMap->cend(); it++) {
    minOpenTime = std::min<int64_t>(minOpenTime, it->second.openTimeSec);
  }

  return CacheAgeStats{
      .maxAgeSecs = std::max<int64_t>(getCurrentTimeSec() - minOpenTime, 0)};
}

} // namespace facebook::velox::cache