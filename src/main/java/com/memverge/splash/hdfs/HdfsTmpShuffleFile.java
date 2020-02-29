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

import com.google.common.io.Closeables;
import com.memverge.splash.ShuffleFile;
import com.memverge.splash.TmpShuffleFile;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.UUID;
import lombok.val;
import org.apache.spark.shuffle.ShuffleSpillInfo;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsTmpShuffleFile extends HdfsShuffleFile implements
    TmpShuffleFile {

  private static final Logger log = LoggerFactory
      .getLogger(HdfsTmpShuffleFile.class);

  private static final String TMP_FILE_PREFIX = "tmp-";
  private HdfsShuffleFile commitTarget = null;

  private UUID uuid = null;

  private HdfsTmpShuffleFile(String pathname) {
    super(pathname);
  }

  static HdfsTmpShuffleFile make() {
    val uuid = UUID.randomUUID();
    val tmpPath = folder.getTmpPath();
    val filename = String.format("%s%s", TMP_FILE_PREFIX, uuid.toString());
    val fullPath = String.format("%s/%s", tmpPath, filename);

    val ret = new HdfsTmpShuffleFile(fullPath);
    ret.uuid = uuid;
    return ret;
  }

  static HdfsTmpShuffleFile make(ShuffleFile file) throws IOException {
    if (file == null) {
      throw new IOException("file is null");
    }
    if (!(file instanceof HdfsShuffleFile)) {
      throw new IOException("only accept HdfsShuffleFile");
    }
    val ret = make();
    ret.commitTarget = (HdfsShuffleFile) file;
    return ret;
  }

  @Override
  public TmpShuffleFile create() throws IOException {
    val fs = getFileSystem();
    val file = getFile();
    val parent = file.getParent();
    if (!fs.exists(parent)) {
      boolean created = fs.mkdirs(parent);
      if (!created) {
        log.info("parent folder {} creation return false.",
            parent.toString());
      }
    }
    if (fs.exists(file)) {
      val deleted = fs.delete(file, true);
      log.info("file already exists.  delete file {} (result: {})",
          file.toString(), deleted);
    }
    val output = fs.create(file);
    output.close();
    log.info("file {} created.", getPath());
    return this;
  }

  @Override
  public void swap(TmpShuffleFile other) throws IOException {
    if (!other.exists()) {
      val message = "Can only swap with a uncommitted tmp file";
      throw new IOException(message);
    }

    val otherLocal = (HdfsTmpShuffleFile) other;

    delete();

    val tmpUuid = otherLocal.uuid;
    otherLocal.uuid = this.uuid;
    this.uuid = tmpUuid;

    val tmpFile = this.file;
    this.file = otherLocal.file;
    otherLocal.file = tmpFile;
  }

  @Override
  public HdfsShuffleFile getCommitTarget() {
    return this.commitTarget;
  }

  @Override
  public ShuffleFile commit() throws IOException {
    if (commitTarget == null) {
      throw new IOException("No commit target.");
    } else if (!exists()) {
      create();
    }
    if (commitTarget.exists()) {
      val msg = String.format("commit target %s already exists",
          commitTarget.getPath());
      log.warn(msg);
      throw new FileAlreadyExistsException(msg);
    }
    log.info("commit tmp file {} to target file {}.",
        getPath(), getCommitTarget().getPath());

    rename(commitTarget.getPath());
    return commitTarget;
  }

  @Override
  public void recall() {
    val commitTarget = getCommitTarget();
    if (commitTarget != null) {
      log.info("recall tmp file {} of target file {}.",
          getPath(), commitTarget.getPath());
    } else {
      log.info("recall tmp file {} without target file.", getPath());
    }
    delete();
  }

  @Override
  public OutputStream makeOutputStream() {
    try {
      create();
    } catch (IOException e) {
      val msg = String.format("Create file %s failed.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    OutputStream ret;
    try {
      ret = fs.create(file);
      log.info("create output stream for {}.", getPath());
    } catch (FileNotFoundException e) {
      val msg = String.format("File %s not found?", getPath());
      throw new IllegalArgumentException(msg, e);
    } catch (IOException e) {
      val msg = String.format("Create file %s failed.", getPath());
      throw new IllegalArgumentException(msg, e);
    }
    return ret;
  }

  @Override
  public UUID uuid() {
    return this.uuid;
  }

  private void copyFileStreamNIO(
      FileChannel input,
      FileChannel output,
      long startPosition,
      long bytesToCopy) throws IOException {
    long count = 0L;
    while (count < bytesToCopy) {
      count += input.transferTo(
          count + startPosition,
          bytesToCopy - count,
          output);
    }
  }
}
