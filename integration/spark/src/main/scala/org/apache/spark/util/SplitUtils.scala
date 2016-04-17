/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD, NewHadoopPartition, NewHadoopRDD}
import org.carbondata.core.load.BlockDetails

/**
  * this object use to handle file splits
  */
object SplitUtils {
  /**
    * get file splits,return Array[BlockDetails], if pathList is empty,then return null
    * @param pathList
    * @param sc
    * @return
    */
  def getSplits(pathList: java.util.List[String], sc: SparkContext): Array[BlockDetails] = {
    if (pathList.isEmpty) {
      //case for the partition handle nothing
      null
    } else {
      val path = new StringBuilder
      for (i <- 0 until pathList.size) {
        val str = pathList.get(i).replace('\\', '/');
        path.append(str)
        if (i < pathList.size - 1) {
          path.append(",")
        }
      }
      //clone the hadoop configuration
      val hadoopConfiguration = new Configuration(sc.hadoopConfiguration)
      //set folder or file
      hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", path.toString)
      hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
      val newHadoopRDD = new NewHadoopRDD[LongWritable, Text](sc, classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConfiguration)
      val splits: Array[FileSplit] = newHadoopRDD.getPartitions.map { part => part.asInstanceOf[NewHadoopPartition]
        .serializableHadoopSplit.value.asInstanceOf[FileSplit]
      }
      splits.map {
        block =>
          val blockDetails = new BlockDetails
          blockDetails.setBlockOffset(block.getStart)
          blockDetails.setBlockLength(block.getLength)
          blockDetails.setFilePath(block.getPath.toString)
          blockDetails
      }
    }
  }
}