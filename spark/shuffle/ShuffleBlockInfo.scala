/*
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

package org.apache.spark.shuffle

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.Logging
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * Created by marvin on 15-4-21.
 */

/**
 * driver端生成并发送给executor,用来计算要获取的block的信息
 */
class ShuffleBlockInfo(
  var loc:BlockManagerId,
  var shuffleId:Int,
  var mapId:Int,
  var reduceId:Array[Int],
  var reduceBlockSize:Array[Long],
  var tid:Long) extends Externalizable with Logging {

  protected def this() = this(null.asInstanceOf[BlockManagerId],
    -1, -1, null.asInstanceOf[Array[Int]],null.asInstanceOf[Array[Long]],-1)


  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    //logInfo("******** shuffleBlockInfo   writeExternal  ********")
    loc.writeExternal(out)
    out.writeInt(shuffleId)
    out.writeInt(mapId)
    out.writeInt(reduceId.length)
    //logInfo("******  write len is   ******" + " " + reduceId.length)
    //out.write(reduceId.map(ShuffleBlockInfo.compressSize))
    reduceId.foreach{case x => out.writeInt(x)}
    out.write(reduceBlockSize.map(ShuffleBlockInfo.compressSize))
    out.writeLong(tid)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    //logInfo("******** shuffleBlockInfo   readExternal   ********")
    loc = BlockManagerId(in)
    shuffleId = in.readInt()
    mapId = in.readInt()
    val len = in.readInt()
    //logInfo("******  read len is   ******" + " " + len)
    val reduceIdInt = new ArrayBuffer[Int]
    for(x <- 0 to len-1){
      val tmp = in.readInt()
      reduceIdInt.append(tmp)
    }
    reduceId = reduceIdInt.toArray
   /* val reduceIdByte = new Array[Byte](len)
    in.readFully(reduceIdByte)
    reduceId = reduceIdByte.map(ShuffleBlockInfo.decompressSizeToInt)*/
    val reduceBlockSizeByte = new Array[Byte](len)
    in.readFully(reduceBlockSizeByte)
    reduceBlockSize = reduceBlockSizeByte.map(ShuffleBlockInfo.decompressSize)
    tid = in.readLong()
   // logInfo("****** reduceBlockSize ******" + " " + reduceBlockSize(0))
  }


}


object ShuffleBlockInfo {

    private[this] val LOG_BASE = 1.1

    /**
     * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
     * We do this by encoding the log base 1.1 of the size as an integer, which can support
     * sizes up to 35 GB with at most 10% error.
     */
    def compressSize(size: Long): Byte = {
      if (size == 0) {
        0
      } else if (size <= 1L) {
        1
      } else {
        math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
      }
    }

  def compressSize(size: Int): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }


    /**
     * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
     */
    def decompressSize(compressedSize: Byte): Long = {
      if (compressedSize == 0) {
        0
      } else {
        math.pow(LOG_BASE, compressedSize & 0xFF).toLong
      }
    }

  def decompressSizeToInt(compressedSize: Byte): Int = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toInt
    }
  }






}
