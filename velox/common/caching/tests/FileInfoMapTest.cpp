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

#include "velox/common/caching/FileInfoMap.h"
#include "velox/common/base/Fs.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include "gtest/gtest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::velox::cache {

class FileInfoMapTest : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
  }
};

TEST_F(FileInfoMapTest, createAndRelease) {
  EXPECT_FALSE(FileInfoMap::exists());

  FileInfoMap::create();
  EXPECT_TRUE(FileInfoMap::exists());

  FileInfoMap::release();
  EXPECT_FALSE(FileInfoMap::exists());
}

TEST_F(FileInfoMapTest, resetOpenTimeSec) {
  FileInfoMap::create();
  FileInfoMap::getInstance()->addOpenFileInfo(101);
  auto t1 = FileInfoMap::getInstance()->find(101)->openTimeSec;

  // Test addOpenFileInfo does not overwrite openTimeSec of existing entries.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  FileInfoMap::getInstance()->addOpenFileInfo(101);
  auto t2 = FileInfoMap::getInstance()->find(101)->openTimeSec;
  EXPECT_EQ(t1, t2);

  // Test addOpenFileInfo resets openTimeSec after one entry is deleted.
  FileInfoMap::getInstance()->deleteNotCached();
  FileInfoMap::getInstance()->addOpenFileInfo(101);
  auto t3 = FileInfoMap::getInstance()->find(101)->openTimeSec;
  EXPECT_LT(t1, t3);
}

} // namespace facebook::velox::cache
