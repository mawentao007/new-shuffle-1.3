package org.apache.spark.shuffle

import java.nio.file.Files

import org.apache.spark.executor.CoarseGrainedExecutorBackend
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}

/**
 * Created by marvin on 15-4-22.
 * modify from ShuffleBlockFetcherIterator.scala
 * 用于进行块的预取，只负责处理一个map task对应到本executor的数据，因此对每个task都有一个实例负责（可以统一为一个？暂时不行）
 */
private[spark] class ShuffleBlockTransporter(
  coarseGrainedExecutorBackend: CoarseGrainedExecutorBackend,
  shuffleBlockInfo:ShuffleBlockInfo,
  blockManager: BlockManager,
  conf: SparkConf
) extends Logging{

  private[this] val fetchRequests = new Queue[FetchRequest]

  private[this] var bytesInFlight = 0L

  private[this] val maxBytesInFlight = SparkEnv.get.conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024

  private[this] var numBlocksToFetch:Int = 0

  private[this] val remoteBlocks = new HashSet[BlockId]()

  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  //private[this] val results = new LinkedBlockingQueue[FetchResult]

  private[this] val shuffleClient = blockManager.shuffleClient

  private[this]  var finishedWriteBlockNumber = 0

  private[this]  var numberOfBlocksToFetch = 0




  private[spark] def run(): Unit = {
    logInfo("%%%%%% what the fuck in shuffleInfo " + shuffleBlockInfo.loc + " " + shuffleBlockInfo.reduceId.toList + " %%%%%%")

    // Split local and remote blocks.
    val remoteRequests = splitLocalRemoteBlocks()

    fetchRequests ++= Utils.randomize(remoteRequests)

    numberOfBlocksToFetch = fetchRequests.length
    logInfo("%%%%%% numberOfBlocksToFetch " + numberOfBlocksToFetch + " %%%%%%")

    if(numberOfBlocksToFetch == 0){
      coarseGrainedExecutorBackend.infoDriverPushFinished(shuffleBlockInfo)
    }else {
      while (fetchRequests.nonEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
        sendRequest(fetchRequests.dequeue())
      }
    }

  }

  val blocksByAddress:Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    /*val blocksByAddress = new ArrayBuffer[(BlockManagerId,ArrayBuffer[(BlockId,Long)])]() */
    val idSizeCouple  = new ArrayBuffer[(BlockId,Long)]()
    for((reduceId,reduceBlockSize) <- shuffleBlockInfo.reduceId.zip(shuffleBlockInfo.reduceBlockSize)){
      val shuffleBlockId = new ShuffleBlockId(shuffleBlockInfo.shuffleId, shuffleBlockInfo.mapId, reduceId)
      //logInfo("%%%%%% [ShuffleBlockTransporter] ShuffleBlockId is " + shuffleBlockId + " %%%%%%")
      idSizeCouple ++=mutable.Map( shuffleBlockId -> reduceBlockSize)
    }
    val ans:Seq[(BlockManagerId,Seq[(BlockId,Long)])] = new ArrayBuffer()
    ans++Map(shuffleBlockInfo.loc->idSizeCouple)
  }

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }

        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }

    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }


  private[this] def sendRequest(req: FetchRequest) {
    logInfo("%%%%%% [ShuffleBlockTransporter  blockId is] " + req.address + req.blocks.toList + " %%%%%%")
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val blockIds = req.blocks.map(_._1.toString)

    val address = req.address
    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
            buf.retain()
            val blockFile = blockManager.diskBlockManager.getFile(blockId)
            if (blockFile.exists) {
              if (blockFile.delete()) {
                logInfo(s"Removed existing shuffle file $blockFile")
              } else {
                logWarning(s"Failed to remove existing shuffle file $blockFile")
              }
            }

            val destination = blockFile.toPath
            val inputStream = buf.createInputStream()
            Files.copy(inputStream, destination)
            inputStream.close()

            logInfo("%%%%%% write Finished " + blockId + " %%%%%%")
            buf.release()
            finishedWriteBlockNumber += 1
            if(finishedWriteBlockNumber == numberOfBlocksToFetch) {
              coarseGrainedExecutorBackend.infoDriverPushFinished(shuffleBlockInfo)
            }
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          //results.put(new FailureFetchResult(BlockId(blockId), e))
          logInfo("%%%%%% [ShuffleBlockTransporter]  blockFetchSFailure BlockId " + blockId  + " %%%%%%")
        }
      }
    )
  }




  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }


  private[spark] sealed trait FetchResult {
    val blockId: BlockId
  }

  private[spark] case class SuccessFetchResult(blockId: BlockId, size: Long, buf: ManagedBuffer)
    extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }


  private[spark] case class FailureFetchResult(blockId: BlockId, e: Throwable)
    extends FetchResult

}
