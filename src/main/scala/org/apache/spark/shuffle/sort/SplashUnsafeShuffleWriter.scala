/*
 * Modifications copyright (C) 2018 MemVerge Inc.
 *
 * Modified to use the IO interface class.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle.sort

import java.io._

import com.google.common.io.Closeables
import com.memverge.splash.TmpShuffleFile
import org.apache.commons.io.output.CountingOutputStream
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.storage.TimeTrackingOutputStream
import org.apache.spark.{SparkEnv, TaskContext}

private[spark] class SplashUnsafeShuffleWriter[K, V](
    resolver: SplashShuffleBlockResolver,
    handle: SplashSerializedShuffleHandle[K, V],
    mapId: Int,
    context: TaskContext,
    serializer: SplashSerializer)
    extends ShuffleWriter[K, V] with Logging {
  private val conf = SparkEnv.get.conf
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val shuffleId = dep.shuffleId

  private var peakMemoryUsedBytes = 0L
  private var stopping = false
  private var partitionLengths: Array[Long] = Array()

  private var mapStatus: MapStatus = _

  private var sorter = new SplashUnsafeSorter(
    context,
    partitioner.numPartitions,
    conf,
    serializer)
  private var spilled = 0

  private def updatePeakMemoryUsed(): Unit = {
    if (sorter != null) {
      val mem = sorter.getPeakMemoryUsedBytes
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem
      }
    }
  }

  def getSpilled: Int = if (sorter != null) sorter.getSpilled else spilled

  def getPeakMemoryUsedBytes: Long = {
    updatePeakMemoryUsed()
    peakMemoryUsedBytes
  }

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    var success = false
    try {
      writeRecords(records)
      closeAndWriteOutput()
      success = true
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources()
        } catch {
          case e: Exception =>
            // Only throw this error if we won't be masking another error.
            if (success) {
              throw e
            } else {
              logError("In addition to a failure during writing, we " +
                  "failed during cleanup.", e)
            }
        }
      }
    }
  }

  private[sort] def writeRecords(records: Iterator[Product2[K, V]]): Unit = {
    while (records.hasNext) {
      insertRecordIntoSorter(records.next)
    }
  }

  def closeAndWriteOutput(): Unit = {
    assert(sorter != null)
    updatePeakMemoryUsed()
    val spills = sorter.closeAndGetSpills()
    spilled = spills.length
    sorter = null
    val dataTmp = resolver.getDataTmpFile(shuffleId, mapId)
    try {
      try {
        partitionLengths = mergeSpills(spills, dataTmp)
      } catch {
        case e: Exception =>
          logError("mergeSpills raise exception.", e)
      } finally {
        spills.foreach(_.file.delete())
      }
      resolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, dataTmp)
    } finally {
      dataTmp.recall()
      mapStatus = MapStatus(resolver.blockManagerId, partitionLengths)
    }
  }

  private[spark] def getPartitionLengths: Array[Long] = partitionLengths

  private[spark] def insertRecordIntoSorter(record: Product2[K, V]): Unit = {
    assert(sorter != null)
    val partitionId = partitioner.getPartition(record._1)
    sorter.insertRecord(record, partitionId)
  }

  private[spark] def wrap(is: InputStream): InputStream = serializer.wrap(is)

  private[spark] def forceSorterToSpill(): Unit = sorter.spill()

  def mergeSpills(spills: Array[ShuffleSpillInfo], dataTmp: TmpShuffleFile): Array[Long] = {
    val compressionCodec = CompressionCodec.createCodec(conf)
    val fastMergeEnabled = conf.get(SplashOpts.fastMergeEnabled)
    val fastMergeSupported = serializer.isFastMergeSupported && dataTmp.supportFastMerge

    logInfo(s"merge ${spills.length} with ${partitioner.numPartitions} partitions.")
    try {
      if (spills.length == 0) {
        new Array[Long](partitioner.numPartitions)
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        val firstSpill = spills(0).file
        logDebug(s"swap temp file, change ${firstSpill.uuid()}'s " +
            s"target to ${dataTmp.getCommitTarget.getPath}")
        dataTmp.swap(firstSpill)
        spills(0).partitionLengths
      } else {
        val partitionLengths = if (fastMergeEnabled && fastMergeSupported) {
          logDebug("Using fast merge")
          dataTmp.fastMerge(spills)
        } else {
          logDebug("Using slow merge")
          val compressionCodecOpt = if (serializer.isCompressEnabled) {
            Some(compressionCodec)
          } else {
            None
          }
          mergeSpillsWithStream(spills, dataTmp, compressionCodecOpt)
        }
        writeMetrics.decBytesWritten(spills(spills.length - 1).spillSize)
        writeMetrics.incBytesWritten(partitionLengths.sum)
        partitionLengths
      }
    } catch {
      case e: IOException =>
        dataTmp.recall()
        throw e
    }
  }

  private def mergeSpillsWithStream(
      spills: Array[ShuffleSpillInfo],
      tmpData: TmpShuffleFile,
      compressionCodecOpt: Option[CompressionCodec]) = {
    val numPartitions = partitioner.numPartitions
    val partitionLengths = new Array[Long](numPartitions)

    val mergedOs = new CountingOutputStream(tmpData.makeOutputStream())

    var threwException = true
    var spillIss: Seq[InputStream] = Seq.empty
    try {
      spillIss = spills.indices.map(spills(_).file.makeInputStream())
      (0 until numPartitions).foreach { partition =>
        val initialFileLength = mergedOs.getByteCount
        val partitionOutput: OutputStream =
          serializer.wrap(
            CloseAndFlushShieldOutputStream(
              new TimeTrackingOutputStream(writeMetrics, mergedOs)),
            compressionCodecOpt)

        val buffer = new Array[Byte](tmpData.getBufferSize)
        spills.indices.foreach { i =>
          val partitionLengthInSpill = spills(i).partitionLengths(partition)
          if (partitionLengthInSpill > 0) {
            SplashUtils.withResources {
              serializer.wrap(
                new LimitedInputStream(
                  spillIss(i),
                  partitionLengthInSpill.toInt,
                  false),
                compressionCodecOpt)
            }(SplashUtils.copy(_, partitionOutput, buffer))
          }
        }
        partitionOutput.flush()
        partitionOutput.close()
        val bytesWrittenToMergedFile = mergedOs.getByteCount - initialFileLength
        partitionLengths(partition) = bytesWrittenToMergedFile
      }
      threwException = false
    } finally {
      spillIss.foreach(Closeables.close(_, true))
      // To avoid masking exceptions that caused us to prematurely enter the
      // finally block, only throw exceptions during cleanup if
      // threwException == false.
      Closeables.close(mergedOs, threwException)
    }
    partitionLengths
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      context.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes)

      if (stopping) {
        Option.apply(null)
      } else {
        stopping = true
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("cannot call stop(true) without having called write()")
          }
          Option.apply(mapStatus)
        } else {
          Option.apply(null)
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in
        // response to an error, so we need to clean up memory and spill files
        // created by the sorter
        sorter.cleanupResources()
      }
    }
  }
}
