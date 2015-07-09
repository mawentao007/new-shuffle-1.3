package org.apache.spark.scheduler.cluster

import org.apache.spark.Logging
import org.apache.spark.scheduler.{DirectTaskResult, MapStatus, MapStatusWrapper, TaskSetManager}
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.storage.BlockManagerId

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by marvin on 15-4-24.
 */
private[spark] class BlockDestinationScheduler(
           coarseGrainedSchedulerBackend: CoarseGrainedSchedulerBackend,
           taskSetManager: TaskSetManager,
           executorDataMap: HashMap[String, ExecutorData],
           blockManagerIdByExecutor:HashMap[String, BlockManagerId]
           ) extends Logging{


  private[this] val tidToResult = new HashMap[Long,DirectTaskResult[_]]

  private[this] val tidToMapStatusGroup = new HashMap[Long,Array[MapStatus]]

  private[this] val tidToExecutorIdGroup = new HashMap[Long,ArrayBuffer[String]]

  private[this] def addTidToResult(tid:Long,result:DirectTaskResult[_]) = {
    tidToResult.put(tid,result)
  }

  private[this] def removeTidToResult(tid:Long) = {
    tidToResult.remove(tid)
  }

  def handleTaskPush(tid:Long,
                     taskPartitionId:Int,
                     result:DirectTaskResult[_],
                     mapStatusWrapper: MapStatusWrapper): Unit ={
    coarseGrainedSchedulerBackend.registerTidToBlockDestinationScheduler(tid,
                        this)
    addTidToResult(tid,result)
    val executorIdToPartitions = generateExecutorIdToPartitions(
      mapStatusWrapper.mapStatus.getReduceTaskNum)
    generateTidToExecutorGroup(executorIdToPartitions,tid)
    val executorIdToShuffleBlockInfo = generateShuffleBlockInfo(executorIdToPartitions,
      tid,mapStatusWrapper,taskPartitionId)
    val mapStatusGroup = generateMapStatusGroup(executorIdToPartitions,mapStatusWrapper)
    tidToMapStatusGroup.put(tid,mapStatusGroup)

    coarseGrainedSchedulerBackend.broadResult(executorIdToShuffleBlockInfo)

  }

  def handleTaskPushResult(tid:Long,executorId:String): Unit ={
    val executorIdGroup = tidToExecutorIdGroup(tid)
    if(executorIdGroup.contains(executorId)){
      executorIdGroup -= executorId
    }else{
      logInfo("executorId not in this group")
    }

    if(executorIdGroup.length == 0){
      val mapStatusGroup = tidToMapStatusGroup(tid)
      val result = tidToResult(tid)
      removeTidToResult(tid)
      taskSetManager.handleTashPushFinished(tid,result,mapStatusGroup)
    }

  }


  private[this] def generateTidToExecutorGroup(
                 executorIdToPartitions:HashMap[String,ArrayBuffer[Int]],
                 tid:Long ): Unit ={
    val executorIdGroup = new ArrayBuffer[String]
    executorIdToPartitions.map{case(executorId,b) => executorId}.foreach{
      case e => executorIdGroup.append(e)
    }
    tidToExecutorIdGroup.put(tid,executorIdGroup)

  }






  private[this] def generateExecutorIdToPartitions(
                                   partitionNum: Int):HashMap[String,ArrayBuffer[Int]] = {
    val executorIdToPartitions = new HashMap[String,ArrayBuffer[Int]]
    val len = executorDataMap.size
    val executorDataMapArray = executorDataMap.toArray
    for (i <- 0 to partitionNum - 1) {        //对hashmap长度取模,根据下标映射，executor正常情况下id映射的是固定的
      executorIdToPartitions.getOrElseUpdate(executorDataMapArray(i % len)._1,ArrayBuffer[Int]()) += i
    }
    executorIdToPartitions
  }



  private[this] def generateShuffleBlockInfo(
                           executorIdToPartitions:HashMap[String,ArrayBuffer[Int]],
                           tid:Long,
                           mapStatusWrapper:MapStatusWrapper,
                           mapTaskId:Int
                           ): HashMap[String,ShuffleBlockInfo]= {

    val executorIdToShuffleBlockInfo = new HashMap[String,ShuffleBlockInfo]
    for ((executorId, reduceTaskIds) <- executorIdToPartitions) {
      val blockManagerId = mapStatusWrapper.mapStatus.location
      val reduceTaskData = new ArrayBuffer[Long]
      reduceTaskIds.foreach { a: Int => reduceTaskData += mapStatusWrapper.mapStatus.getSizeForBlock(a)}
      executorIdToShuffleBlockInfo.put(executorId, new ShuffleBlockInfo(blockManagerId,
        mapStatusWrapper.shuffleId, mapTaskId, reduceTaskIds.toArray, reduceTaskData.toArray,tid))
    }
    executorIdToShuffleBlockInfo
  }


  //生成task到executor的映射


  private[this] def generateMapStatusGroup(
                         executorIdToPartitions:HashMap[String,ArrayBuffer[Int]],
                         mapStatusWrapper:MapStatusWrapper
                         ):Array[MapStatus]={
    val aSize = mapStatusWrapper.mapStatus.getReduceTaskNum
    val mapStatusGroup = new ArrayBuffer[MapStatus]()
    val blockManagerIdToArraySize = new HashMap[BlockManagerId,Array[Long]]()
    for((eId,arrayTId) <-executorIdToPartitions ){
      val longOfblock = Array.fill(aSize)(0L)
      for(tId <- arrayTId){
        longOfblock(tId) = mapStatusWrapper.mapStatus.getSizeForBlock(tId)
      }
      blockManagerIdToArraySize(blockManagerIdByExecutor(eId)) = longOfblock
    }
    for(a <- blockManagerIdToArraySize){
      mapStatusGroup += MapStatus(a._1,a._2)
    }
    mapStatusGroup.toArray
  }



}
