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


package org.carbondata.integration.spark.rdd

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext,SerializableWritable}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.common.logging.impl.StandardLogService
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark.Result
import org.carbondata.integration.spark.load.{CarbonLoadModel, CarbonLoaderUtil}
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.{CarbonQueryUtil, CarbonSparkInterFaceLogEvent}
import org.carbondata.processing.constants.DataProcessorConstants
import org.carbondata.processing.etl.DataLoadingException
import org.carbondata.processing.graphgenerator.GraphGenerator
import org.carbondata.query.datastorage.InMemoryTableStore

import scala.collection.JavaConversions._

class CarbonPartitionLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit, val blocksDetails: Array[BlockDetails])
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)
  val partitionBlocksDetail = blocksDetails

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
  * this class is use for this rdd to partitioin
  * @param rddId
  * @param idx
  * @param host
  * @param blocksDetails
  */
class CarbonNodeLoadPartition(rddId: Int, val idx: Int, host: String, val blocksDetails: Array[BlockDetails])
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = host
  val nodeBlocksDetail = blocksDetails

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
  * Use this RDD class to load data
  * @param sc
  * @param result
  * @param carbonLoadModel
  * @param storeLocation
  * @param hdfsStoreLocation
  * @param kettleHomePath
  * @param partitioner
  * @param columinar
  * @param currentRestructNumber
  * @param loadCount
  * @param cubeCreationTime
  * @param schemaLastUpdatedTime
  * @param blocksGroupBy
  * @param isPartition
  * @tparam K
  * @tparam V
  */
class CarbonDataLoadRDD[K, V](
                           sc: SparkContext,
                           result: Result[K, V], carbonLoadModel: CarbonLoadModel,
                           var storeLocation: String,
                           hdfsStoreLocation: String,
                           kettleHomePath: String,
                           partitioner: Partitioner,
                           columinar: Boolean,
                           currentRestructNumber: Integer,
                           loadCount: Integer,
                           cubeCreationTime: Long,
                           schemaLastUpdatedTime: Long,
                           blocksGroupBy: Array[(String,Array[BlockDetails])],
                           isPartition: Boolean
                         )
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    isPartition match {
      case true =>
        //for table split partition
        var splits = Array[TableSplit]()
        if (carbonLoadModel.isDirectLoad()) {
          splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath(), partitioner.nodeList, partitioner.partitionCount)
        }
        else {
          splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getSchemaName(), carbonLoadModel.getCubeName(), null, partitioner)
        }
        //
        val result = new Array[Partition](splits.length)
        for (i <- 0 until result.length) {
          //filter the same partition unique id, because only one will match, so get 0 element
          val blocksDetails:Array[BlockDetails] = blocksGroupBy.filter(p => p._1 == splits(i).getPartition.getUniqueID)(0)._2
          result(i) = new CarbonPartitionLoadPartition(id, i, splits(i),blocksDetails)
        }
        result
      case false =>
        //for node partition
        val result = new Array[Partition](blocksGroupBy.length)
        for (i <- 0 until result.length) {
          result(i) = new CarbonNodeLoadPartition(id, i,blocksGroupBy(i)._1,blocksGroupBy(i)._2)
        }
        result
    }
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      //var loadCount = 0
      var partitionID = "0"
      var model: CarbonLoadModel = _

      try {
        val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
        if (null == carbonPropertiesFilePath) {
          System.setProperty("carbon.properties.filepath", System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties");
        }

        isPartition match {
          case true =>
            //for table split partition
            val split = theSplit.asInstanceOf[CarbonPartitionLoadPartition]
            logInfo("Input split: " + split.serializableHadoopSplit.value)
            val blocksID = carbonLoadModel.getSchemaName + "_" + carbonLoadModel.getCubeName + "_" +
              split.serializableHadoopSplit.value.getPartition.getUniqueID +"_" + UUID.randomUUID()
            if (carbonLoadModel.isDirectLoad()) {
              model = carbonLoadModel.getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID(),
                split.serializableHadoopSplit.value.getPartition().getFilesPath, carbonLoadModel.getCsvHeader(), carbonLoadModel.getCsvDelimiter(), blocksID)
            } else {
              model = carbonLoadModel.getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID(), blocksID)
            }
            partitionID = split.serializableHadoopSplit.value.getPartition().getUniqueID()
            //get this partition data blocks and put it to global static map
            GraphGenerator.blockInfo.put(blocksID, split.partitionBlocksDetail)
            StandardLogService.setThreadName(partitionID, null)
          case false =>
            //for node partition
            val split = theSplit.asInstanceOf[CarbonNodeLoadPartition]
            logInfo("Input split: " + split.serializableHadoopSplit)
            logInfo("The Block Count in this node :" + split.nodeBlocksDetail.length)
            val blocksID = carbonLoadModel.getSchemaName + "_" + carbonLoadModel.getCubeName +
              "_" + UUID.randomUUID()
            //set this node blocks info to global static map
            GraphGenerator.blockInfo.put(blocksID, split.nodeBlocksDetail)
            if (carbonLoadModel.isDirectLoad()) {
              val filelist: java.util.List[String] = new java.util.ArrayList[String](CarbonCommonConstants.CONSTANT_SIZE_TEN)
              CarbonQueryUtil.getAllFiles(carbonLoadModel.getFactFilePath, filelist, FileFactory.getFileType(carbonLoadModel.getFactFilePath))
              model = carbonLoadModel.getCopyWithPartition(partitionID, filelist, carbonLoadModel.getCsvHeader(), carbonLoadModel.getCsvDelimiter(), blocksID)
            }
            else {
              model = carbonLoadModel.getCopyWithPartition(partitionID, blocksID)
            }
            StandardLogService.setThreadName(blocksID, null)
        }

        CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true")
        CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1")
        CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true")
        CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true")
        CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true")
        CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000")
        CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false")
        CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000")
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
          storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
        }

        dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS

        if (model.isRetentionRequest()) {
          recreateAggregationTableForRetention
        }
        else if (model.isAggLoadRequest()) {
          dataloadStatus = createManualAggregateTable
        }
        else {
          try {
            CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath, currentRestructNumber);
            //loadCount = CarbonLoaderUtil.getLoadCount(model, currentRestructNumber);
          } catch {
            case e: DataLoadingException => if (e.getErrorCode == DataProcessorConstants.BAD_REC_FOUND) {
              //loadCount = CarbonLoaderUtil.getLoadCount(model, currentRestructNumber);
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              logInfo("Bad Record Found")
            } else {
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
              LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
            }
            case e: Exception =>
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
              LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
          } finally {
            if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val newSlice = CarbonCommonConstants.LOAD_FOLDER + loadCount
              var isCopyFailed = false
              try {
                CarbonLoaderUtil.copyCurrentLoadToHDFS(model, currentRestructNumber, newSlice, null, currentRestructNumber);
              } catch {
                case e: Exception =>
                  isCopyFailed = true
                  dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
                  LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
              }
              if (!isCopyFailed)
                dataloadStatus = checkAndLoadAggregationTable
              if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
                logInfo("DataLoad failure")
              } else {
                logInfo("DataLoad complete")
                logInfo("Data Loaded successfully with LoadCount:" + loadCount)
              }
            } else {
              logInfo("DataLoad failure")
            }
          }
        }

      } catch {
        case e: Exception =>
          dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          logInfo("DataLoad failure")
      }

      def checkAndLoadAggregationTable(): String = {
        val schema = model.getCarbonDataLoadSchema
        val aggTables =schema.getCarbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.toSeq.toArray
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + loadCount
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders);
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          var listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
          listOfAllLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfAllLoadFolders)
          val copyListOfLoadFolders = listOfLoadFolders.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.toList
          loadCubeSlices(listOfAllLoadFolders, details)
          var loadFolders = Array[String]()
          val loadFolder = CarbonLoaderUtil.getAggLoadFolderLocation(newSlice, model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, currentRestructNumber)
          if (null != loadFolder) {
            loadFolders :+= loadFolder
          }
          dataloadStatus = iterateOverAggTables(aggTables, copyListOfLoadFolders, copyListOfUpdatedLoadFolders, loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            // remove the current slice from memory not the cube
            CarbonLoaderUtil.removeSliceFromMemory(model.getSchemaName, model.getCubeName, newSlice)
            logInfo(s"Aggregate table creation failed")
          }
          else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        return dataloadStatus
      }

      def loadCubeSlices(listOfAllLoadFolders: java.util.List[String], loadMetadataDetails: Array[LoadMetadataDetails]) = {
        CarbonProperties.getInstance().addProperty("carbon.cache.used", "false");
        val cube = InMemoryTableStore.getInstance.loadCubeMetadataIfRequired(model.getSchema, model.getSchema.cubes(0), null, schemaLastUpdatedTime)
        CarbonQueryUtil.createDataSource(currentRestructNumber, model.getSchema, cube, null, listOfAllLoadFolders, model.getTableName, hdfsStoreLocation, cubeCreationTime, loadMetadataDetails)
      }

      def createManualAggregateTable(): String = {
        val details = model.getLoadMetadataDetails.toSeq.toArray
        val listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
        val listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
        val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
        loadCubeSlices(listOfAllLoadFolders, details)
        var loadFolders = Array[String]()
        var restructFolders = Array[String]()
        for (number <- 0 to currentRestructNumber) {
          restructFolders = CarbonLoaderUtil.getStorelocs(model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, number)
          loadFolders = loadFolders ++ restructFolders
        }
        val aggTable = model.getAggTableName
        dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
        if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
          logInfo(s"Aggregate table creation failed :: $aggTable")
        } else {
          logInfo(s"Aggregate table creation successfull :: $aggTable")
        }
        dataloadStatus
      }

      def recreateAggregationTableForRetention() = {
        val schema = model.getCarbonDataLoadSchema
        val aggTables = schema.getCarbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.toSeq.toArray
          val listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          val listOfAllLoadFolder = CarbonQueryUtil.getListOfSlices(details)
          loadCubeSlices(listOfAllLoadFolder, details)
          var loadFolders = Array[String]()
          listOfUpdatedLoadFolders.foreach { sliceNum =>
            val newSlice = CarbonCommonConstants.LOAD_FOLDER + sliceNum
            val loadFolder = CarbonLoaderUtil.getAggLoadFolderLocation(newSlice, model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, currentRestructNumber)
            if (null != loadFolder) {
              loadFolders :+= loadFolder
            }
          }
          iterateOverAggTables(aggTables, listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
        }
      }
      //TO-DO Aggregate table needs to be handled
      def iterateOverAggTables(aggTables: java.util.List[String], listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
        model.setAggLoadRequest(true)
        aggTables.foreach { aggTable =>
          model.setAggTableName(aggTable)
          dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            logInfo(s"Aggregate table creation failed :: aggTable")
            return dataloadStatus
          }
        }
        return dataloadStatus
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
        var levelCacheKeys: scala.collection.immutable.List[String] = Nil
        if (InMemoryTableStore.getInstance.isLevelCacheEnabled()) {
          val columnList = CarbonLoaderUtil.getColumnListFromAggTable(model)
          val details = model.getLoadMetadataDetails.toSeq.toArray
          levelCacheKeys = CarbonQueryUtil.loadRequiredLevels(CarbonQueryUtil.getListOfSlices(details), model.getSchemaName + '_' + model.getCubeName, columnList).toList
        }
        loadFolders.foreach { loadFolder =>
          val restructNumber = CarbonUtil.getRestructureNumber(loadFolder, model.getTableName)
          try {
            if (CarbonLoaderUtil.isSliceValid(loadFolder, listOfLoadFolders, listOfUpdatedLoadFolders, model.getTableName)) {
              model.setFactStoreLocation(loadFolder)
              CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath, currentRestructNumber)
            } else {
              CarbonLoaderUtil.createEmptyLoadFolder(model, loadFolder, hdfsStoreLocation, restructNumber)
            }
          } catch {
            case e: Exception =>
              LogServiceFactory.getLogService.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } finally {
            updateLevelCacheStatus(levelCacheKeys)
            if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val loadName = loadFolder.substring(loadFolder.indexOf(CarbonCommonConstants.LOAD_FOLDER))
              try {
                CarbonLoaderUtil.copyCurrentLoadToHDFS(model, restructNumber, loadName, listOfUpdatedLoadFolders, currentRestructNumber)
              }
              catch {
                case e: Exception =>
                  LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
                  return CarbonCommonConstants.STORE_LOADSTATUS_FAILURE;
              }
            } else {
              logInfo(s"Load creation failed :: $loadFolder")
              return dataloadStatus
            }
          }
        }
        return dataloadStatus
      }

      def updateLevelCacheStatus(levelCacheKeys: scala.collection.immutable.List[String]) = {
        levelCacheKeys.foreach { key =>
          InMemoryTableStore.getInstance.updateLevelAccessCountInLRUCache(key)
        }
      }

      var finished = false

      override def hasNext: Boolean = {

        if (!finished) {
          finished = true
          finished
        }
        else {
          !finished
        }
      }

      override def next(): (K, V) = {
        val loadMetadataDetails = new LoadMetadataDetails()
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(dataloadStatus.toString())
        result.getKey(loadCount, loadMetadataDetails)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    isPartition match {
      case true =>
        //for table split partition
        val theSplit = split.asInstanceOf[CarbonPartitionLoadPartition]
        val s = theSplit.serializableHadoopSplit.value.getLocations
        s
      case false =>
        //for node partition
        val theSplit = split.asInstanceOf[CarbonNodeLoadPartition]
        val s: Seq[String] = List(theSplit.serializableHadoopSplit)
        logInfo("Prefered Location for split : " + s(0))
        s
    }
  }
}

