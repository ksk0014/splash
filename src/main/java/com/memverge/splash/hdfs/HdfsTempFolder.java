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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.SplashOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsTempFolder {

  private static final Logger log = LoggerFactory.getLogger(HdfsTempFolder.class);

  private static final HdfsTempFolder INSTANCE = new HdfsTempFolder();

  private FileSystem fs;

  public static HdfsTempFolder getInstance() {
    return INSTANCE;
  }

  private HdfsTempFolder() {
    try {
      Configuration config = new Configuration();
      fs = FileSystem.get(config);
    } catch (IOException e) {
      log.error("get filesystem failed.", e);
    }
  }

  private String getSplashPath() {
    String folder = null;
    SparkEnv env = SparkEnv.get();
    if (env != null) {
      SparkConf conf = env.conf();
      if (conf != null) {
        folder = conf.get(SplashOpts.localSplashFolder());
      }
    }
    return folder;
  }

  private String getUser() {
    String user = System.getProperty("user.name");
    return StringUtils.isEmpty(user) ? "unknown" : user;
  }

  public String getTmpPath() {
    String folder = getSplashPath();
    Path path = new Path(folder, "tmp");
    ensureFolderExists(path);
    return path.toString();
  }

  public String getShufflePath(String appId) {
    String folder = getSplashPath();
    Path path = new Path(new Path(folder, appId), "shuffle");
    ensureFolderExists(path);
    return path.toString();
  }

  public int countShuffleFile(String appId) {
    int shuffleFileCount = 0;
    Path file = new Path(getShufflePath(appId));
    try {
      RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(file, true);
      while (listFiles.hasNext()) {
        LocatedFileStatus fileStatus = listFiles.next();
        shuffleFileCount += 1;
      }
    } catch (FileNotFoundException e) {
      log.error("{} not exists.  do nothing.", file.toString());
    } catch (IOException e) {
      log.error("{} listFiles fail.  do nothing.", file.toString());
    }
    return shuffleFileCount;
  }

  public int countTmpFile() {
    int tmpFileCount = 0;
    Path file = new Path(getTmpPath());
    try {
      RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(file, true);
      while (listFiles.hasNext()) {
        LocatedFileStatus fileStatus = listFiles.next();
        tmpFileCount += 1;
      }
    } catch (FileNotFoundException e) {
      log.error("{} not exists.  do nothing.", file.toString());
    } catch (IOException e) {
      log.error("{} listFiles fail.  do nothing.", file.toString());
    }
    return tmpFileCount;
  }

  public void reset() {
    String path = getSplashPath();
    try {
      fs.delete(new Path(path), true);
    } catch (FileNotFoundException e) {
      log.debug("{} not exists.  do nothing.", path);
    } catch (IOException e) {
      log.error("failed to clean up local splash folder: {}", path, e);
    }
  }

  private void ensureFolderExists(Path path) {
    Path folder = path.getParent();
    try {
      if (!fs.exists(folder) && fs.mkdirs(folder)) {
        log.info("Create folder {}", folder.toString());
      }
    } catch (IOException e) {
      log.error("failed to exists and mkdirs folder: {}", path.toString(), e);
    }
  }
}
