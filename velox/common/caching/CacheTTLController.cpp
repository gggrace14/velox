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

#include "velox/common/caching/AsyncDataCache.h"

namespace facebook::velox::cache {

bool CacheTTLController::addOpenFileInfo(
    uint64_t fileNum,
    int64_t openTimeSec) {
  auto lockedFileMap = fileInfoMap_.wlock();
  auto it = lockedFileMap->find(fileNum);
  if (it == lockedFileMap->end() || it->second.removeInProgress) {
    lockedFileMap->insert_or_assign(fileNum, RawFileInfo{openTimeSec, false});
    return true;
  }
  return false;
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

void CacheTTLController::applyTTL(int64_t ttlSecs) {
  int64_t maxOpenTime = getCurrentTimeSec() - ttlSecs;

  auto filesRetained = std::make_shared<folly::F14FastSet<uint64_t>>();
  bool success = cache_.removeFileEntries(
      [&]() { return getAndMarkAgedOutFiles(maxOpenTime); }, filesRetained);

  if (success) {
    removeAgedOutFiles(filesRetained);
  } else {
    reset();
  }
}

std::shared_ptr<const folly::F14FastSet<uint64_t>>
CacheTTLController::getAndMarkAgedOutFiles(int64_t maxOpenTimeSecs) {
  auto lockedFileMap = fileInfoMap_.wlock();

  auto fileNums = std::make_shared<folly::F14FastSet<uint64_t>>();

  for (auto it = lockedFileMap->begin(); it != lockedFileMap->end(); it++) {
    if (it->second.removeInProgress ||
        it->second.openTimeSec < maxOpenTimeSecs) {
      fileNums->insert(it->first);
      it->second.removeInProgress = true;
    }
  }

  return fileNums;
}

void CacheTTLController::removeAgedOutFiles(
    const std::shared_ptr<const folly::F14FastSet<uint64_t>>& filesToRetain) {
  fileInfoMap_.withWLock([&](auto& fileMap) {
    auto it = fileMap.begin();
    while (it != fileMap.end()) {
      if (!it->second.removeInProgress) {
        it++;
        continue;
      }
      if (filesToRetain->find(it->first) != filesToRetain->cend()) {
        it->second.removeInProgress = false;
        it++;
        continue;
      }
      it = fileMap.erase(it);
    }
  });
}

void CacheTTLController::reset() {
  fileInfoMap_.withWLock([](auto& fileMap) {
    for (auto& [_, fileInfo] : fileMap) {
      fileInfo.removeInProgress = false;
    }
  });
}

} // namespace facebook::velox::cache