/*
 * Copyright (C) 2018 MemVerge Inc.
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
package com.memverge.splash.hdfs;

import com.memverge.splash.ShuffleFile;
import com.memverge.splash.ShuffleListener;
import com.memverge.splash.StorageFactory;
import com.memverge.splash.TmpShuffleFile;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class HdfsFactory implements StorageFactory {

  private HdfsTempFolder folder = HdfsTempFolder.getInstance();

  @Override
  public TmpShuffleFile makeSpillFile() throws IOException {
    return HdfsTmpShuffleFile.make();
  }

  @Override
  public TmpShuffleFile makeDataFile(String path) throws IOException {
    return HdfsTmpShuffleFile.make(getDataFile(path));
  }

  @Override
  public TmpShuffleFile makeIndexFile(String path) throws IOException {
    return HdfsTmpShuffleFile.make(new HdfsShuffleFile(path));
  }

  @Override
  public ShuffleFile getDataFile(String path) {
    return new HdfsShuffleFile(path);
  }

  @Override
  public ShuffleFile getIndexFile(String path) {
    return new HdfsShuffleFile(path);
  }

  @Override
  public Collection<ShuffleListener> getListeners() {
    // No listeners for this implementation
    return Collections.emptyList();
  }

  @Override
  public String getShuffleFolder(String appId) {
    return folder.getShufflePath(appId);
  }

  @Override
  public int getShuffleFileCount(String appId) {
    return folder.countShuffleFile(appId);
  }

  @Override
  public int getTmpFileCount() {
    return folder.countTmpFile();
  }

  @Override
  public void reset() {
    folder.reset();
  }
}
