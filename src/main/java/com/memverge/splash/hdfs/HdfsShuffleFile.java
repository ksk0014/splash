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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsShuffleFile implements ShuffleFile {

  private static final Logger log = LoggerFactory
      .getLogger(HdfsShuffleFile.class);

  protected static HdfsTempFolder folder = HdfsTempFolder.getInstance(); 
  protected Path file;  
  protected FileSystem fs;

  HdfsShuffleFile(String pathname) {
    log.info("init shuffle file {}", pathname);
    file = new Path(pathname);
    try {
      Configuration config = new Configuration();
      fs = FileSystem.get(config);
    } catch (IOException e) {
      log.error("get filesystem failed.", e);
    }
  }

  @Override
  public long getSize() {
    long size = 0;
    try {
        FileStatus fileStatus = fs.getFileStatus(file);
        size = fileStatus.getLen();
    } catch (IOException e) {
        log.error("getFileStatus {} failed.", getPath(), e);
    }
    return size;
  }

  @Override
  public boolean delete() {
    log.info("delete file {}", getPath());
    boolean success = true;
    try {
      success = fs.delete(file, true);
    } catch (FileNotFoundException e) {
      log.info("file to delete {} not found", getPath());
    } catch (IOException e) {
      log.error("delete {} failed.", getPath(), e);
      success = false;
    }
    return success;
  }

  @Override
  public boolean exists() {
    log.info("exists file {}", getPath());
    boolean success = true;
    try {
      success = fs.exists(file);
    } catch (IOException e) {
      log.error("exists {} failed.", getPath(), e);
      success = false;
    }
    return success;
  }

  @Override
  public String getPath() {
    return file.toString();
  }

  Path getFile() {
    return file;
  }

  FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public InputStream makeInputStream() {
    InputStream ret;
    try {
      ret = fs.open(file);
      log.info("create input stream for {}.", getPath());
    } catch (FileNotFoundException e) {
      val msg = String.format("Create input stream failed for %s.", getPath());
      throw new IllegalArgumentException(msg, e);
    } catch (IOException e) {
      val msg = String.format("Create input stream failed for %s.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  void rename(String tgtId) throws IOException {
    Path tgtFile = new Path(tgtId);
    Path parent = tgtFile.getParent();
    if (!fs.exists(parent) && !fs.mkdirs(parent)) {
      if (!fs.exists(parent)) {
        val msg = String.format("create parent folder %s failed",
            parent.toString());
        throw new IOException(msg);
      }
    }
    boolean success = fs.rename(file, tgtFile);
    if (success) {
      log.info("rename {} to {}.", getPath(), tgtId);
    } else {
      String msg = String.format("rename %s to %s failed.", getPath(), tgtId);
      throw new IOException(msg);
    }
  }
}
