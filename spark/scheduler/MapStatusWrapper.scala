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
package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Created by marvin on 15-4-21.
 */
private[spark] sealed trait MapStatusWrapper extends Logging{

  /** 要包装的mapstatus
    *
    * @return
    */
  def mapStatus: MapStatus


  /**
   * 要返回的shuffleId
   * @return
   */
  def shuffleId: Int


}


private[spark] class CompressedMapStatusWrapper(
                                                 private[this] var _mapStatus: MapStatus,
                                                 private[this] var _shuffleId: Int)
  extends MapStatusWrapper with Externalizable {

  protected def this() = this(null,-1) // For deserialization only



  override def mapStatus = _mapStatus

  override def shuffleId = _shuffleId

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    logInfo("%%% wrapper writeExternal %%%")
    _mapStatus.asInstanceOf[CompressedMapStatus].writeExternal(out)
    out.writeInt(_shuffleId)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    logInfo("%%% wrapper readExternal %%%")
    _mapStatus = new CompressedMapStatus(null,null.asInstanceOf[Array[Byte]])
    _mapStatus.asInstanceOf[CompressedMapStatus].readExternal(in)
    _shuffleId = in.readInt()
  }
}




private[spark] class HighlyCompressedMapStatusWrapper(
                                                 private[this] var _mapStatus: MapStatus,
                                                 private[this] var _shuffleId: Int)
  extends MapStatusWrapper with Externalizable {

  protected def this() = this(null,-1) // For deserialization only



  override def mapStatus = _mapStatus

  override def shuffleId = _shuffleId

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    logInfo("%%% wrapper writeExternal %%%")
    _mapStatus.asInstanceOf[HighlyCompressedMapStatus].writeExternal(out)
    out.writeInt(_shuffleId)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    logInfo("%%% wrapper readExternal %%%")
    _mapStatus = HighlyCompressedMapStatus(null,null.asInstanceOf[Array[Long]])
    _mapStatus.asInstanceOf[HighlyCompressedMapStatus].readExternal(in)
    _shuffleId = in.readInt()
  }
}






private[spark] object MapStatusWrapper {

  def apply(mapStatus: MapStatus, shuffleId: Int): MapStatusWrapper = {
    if (mapStatus.isInstanceOf[HighlyCompressedMapStatus]) {
      new HighlyCompressedMapStatusWrapper(mapStatus, shuffleId)
    } else {
      new CompressedMapStatusWrapper(mapStatus, shuffleId)
    }
  }
}




