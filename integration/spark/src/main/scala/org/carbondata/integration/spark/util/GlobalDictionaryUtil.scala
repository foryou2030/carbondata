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
package org.carbondata.integration.spark.util

import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions.{asScalaBuffer, asScalaSet, seqAsJavaList}
import scala.language.implicitConversions
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.carbondata.core.carbon.CarbonDef.Schema
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.integration.spark.load.CarbonLoadModel
import org.carbondata.integration.spark.rdd.ColumnPartitioner
import org.carbondata.integration.spark.rdd.CarbonBlockDistinctValuesCombineRDD
import org.carbondata.integration.spark.rdd.CarbonGlobalDictionaryGenerateRDD
import org.apache.spark.Logging
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.util.CarbonProperties
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.writer.CarbonDictionaryWriter
import org.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.carbondata.core.util.CarbonDictionaryUtil
import org.carbondata.core.util.CarbonUtil
import java.io.IOException
import org.carbondata.core.reader.CarbonDictionaryReader
import org.carbondata.core.reader.CarbonDictionaryReaderImpl
import org.carbondata.integration.spark.rdd.DictionaryLoadModel
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration

/**
 * A object which provide a method to generate global dictionary from CSV files.
 *
 * @author: QiangCai
 * @date: Apr 10, 2016 9:59:55 PM
 */
object GlobalDictionaryUtil extends Logging {

  /**
   * find columns which need to generate global dictionary.
   *
   * @param dimensions dimension list of schema
   * @param columns column list of csv file
   * @return: java.lang.String[]
   */
  def pruneColumns(dimensions: Array[String], columns: Array[String]) = {
    val columnBuffer = new ArrayBuffer[String]
    for (dim <- dimensions) {
      breakable {
        for (i <- 0 until columns.length) {
          if (dim.equalsIgnoreCase(columns(i))) {
            columnBuffer += dim
            break
          }
        }
      }
    }
    columnBuffer.toArray
  }

  /**
   * invoke CarbonDictionaryWriter to write dictionary to file.
   *
   * @param model instance of DictionaryLoadModel
   * @param columnIndex the index of current column in column list
   * @param iter distinct value list of dictionary
   */
  def writeGlobalDictionaryToFile(model: DictionaryLoadModel,
                                  columnIndex: Int,
                                  iter: Iterator[String]) = {
    val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(
      model.hdfsLocation, model.table, model.columns(columnIndex), model.isSharedDimension)
    try {
      if (!model.dictFileExists(columnIndex)) {
        writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
      }
      while (iter.hasNext) {
        writer.write(iter.next)
      }
    } finally {
      writer.close
    }
  }

  /**
   * invoke CarbonDictionaryReader to read dictionary from files.
   *
   * @param model
   * @return: scala.Tuple2<scala.collection.mutable.HashSet<java.lang.String>[],boolean[]>
   */
  def readGlobalDictionaryFromFile(model: DictionaryLoadModel) = {
    val dicts = new Array[HashSet[String]](model.columns.length)
    val existDicts = new Array[Boolean](model.columns.length)
    for (i <- 0 until model.columns.length) {
      dicts(i) = new HashSet[String]
      if (model.dictFileExists(i)) {
        val reader: CarbonDictionaryReader = new CarbonDictionaryReaderImpl(
          model.hdfsLocation, model.table, model.columns(i), model.isSharedDimension)
        val values = reader.read
        if (values != null) {
          for (j <- 0 until values.size)
            dicts(i).add(new String(values.get(j)))
        }
      }
      existDicts(i) = !(dicts(i).size == 0)
    }
    (dicts, existDicts)
  }

  /**
   * create a instance of DictionaryLoadModel
   *
   * @param table CarbonTableIdentifier
   * @param columns column list
   * @param hdfsLocation store location in HDFS
   * @param dictfolderPath path of dictionary folder
   * @param isSharedDimension a boolean to mark share dimension or non-share dimension
   * @return: org.carbondata.integration.spark.rdd.DictionaryLoadModel
   */
  def createDictionaryLoadModel(table: CarbonTableIdentifier,
                                columns: Array[String],
                                hdfsLocation: String,
                                dictfolderPath: String,
                                isSharedDimension: Boolean) = {
    //list dictionary file path
    val dictFilePaths = new Array[String](columns.length)
    val dictFileExists = new Array[Boolean](columns.length)
    for (i <- 0 until columns.length) {
      dictFilePaths(i) = CarbonDictionaryUtil.getDictionaryFilePath(table,
        dictfolderPath, columns(i), isSharedDimension)
      dictFileExists(i) = CarbonUtil.isFileExists(dictFilePaths(i))
    }
    new DictionaryLoadModel(table,
      columns,
      hdfsLocation,
      dictfolderPath,
      isSharedDimension,
      dictFilePaths,
      dictFileExists)
  }

  /**
   * append all file path to a String, file path separated by comma 
   */
  def getPathsFromCarbonFile(carbonFile: CarbonFile): String ={
    if(carbonFile.isDirectory()){
      val files = carbonFile.listFiles()
      val stringbuild = new StringBuilder()
      for( j <- 0 until files.size){
        stringbuild.append(getPathsFromCarbonFile(files(j))).append(",")
      }
      stringbuild.substring(0, stringbuild.size - 1)
    }else{
      carbonFile.getPath
    }
  }
  
  /**
   * append all file path to a String, inputPath path separated by comma 
   */
  def getPaths(inputPath: String): String = {
    if(inputPath == null || inputPath.isEmpty){
      inputPath
    }else{
      val stringbuild = new StringBuilder()
      val filePaths = inputPath.split(",")
      for( i <- 0 until filePaths.size ){
        val fileType = FileFactory.getFileType(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), fileType)
        stringbuild.append(getPathsFromCarbonFile(carbonFile)).append(",")
      }
      stringbuild.substring(0, stringbuild.size -1)
    }
  }
  
  /**
   * load CSV files to DataFrame by using datasource "com.databricks.spark.csv"
   *
   * @param sqlContext SQLContext
   * @param filePath file or directory path
   * @return: org.apache.spark.sql.DataFrame
   */
  private def loadDataFrame(sqlContext: SQLContext, filePath: String): DataFrame = {
    val df = new SQLContext(sqlContext.sparkContext).read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("comment", null)
      .load(getPaths(filePath))
    df
  }

  /**
   * check whether global dictionary have been generated successfully or not.
   *
   * @param status Array[(String, String)]
   * @return: void
   */
  private def checkStatus(status: Array[(String, String)]) = {
    if (status.exists(x => CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(x._2))) {
      logError("generate global dictionary files failed")
      throw new Exception("Failed to generate global dictionary files")
    } else {
      logInfo("generate global dictionary successfully")
    }
  }

  /**
   * extract dimension set from hierarchy
   *
   * @param schema
   * @return: scala.collection.mutable.HashSet<java.lang.String>
   */
  private def extractHierachyDimension(schema: Schema): HashSet[String] = {
    val dimensionSet = new HashSet[String]
    val dimensions = schema.cubes(0).dimensions
    for (i <- 0 until dimensions.length) {
      val hierarchies = dimensions(i).getDimension(schema).hierarchies
      for (hierachy <- hierarchies) {
        if (hierachy.relation != null) {
          for (level <- hierachy.levels) {
            dimensionSet += level.column
          }
        }
      }
    }
    dimensionSet
  }

  /**
   * read local dictionary and prune column
   *
   * @param sqlContext
   * @param csvFileColumns
   * @param dimColumns
   * @param dfColumns
   * @param localDictionaryPath
   * @param dicFileTag
   * @return (distinctValues, requireColumns)
   */
  private def ReadLocalDictionaryAndPruneColumns(sqlContext: SQLContext, csvFileColumns: Array[String], dimColumns: List[String],
                                                 dfColumns: List[String], localDictionaryPath: String, dicFileTag: String) =
  {
    // read local dictionary file
    var dictionaryRdd: RDD[(Int, Array[String])] = null
    var distinctValues: Array[(Int, Array[String])] = null
    var fs: FileSystem = null
    try
    {
      val conf = new Configuration()
      val dictionaryPath = new Path(localDictionaryPath)
      fs = dictionaryPath.getFileSystem(conf)
      if(fs.isFile(dictionaryPath))
      {
        dictionaryRdd = sqlContext.sparkContext.textFile(localDictionaryPath)
          .map(x => {val tokens = x.split(","); (tokens(0).toInt, tokens(1))})
          .groupByKey()
          .map(x => (x._1, x._2.toArray))
      }
      else
      {
        for(dictionaryFile <- fs.listStatus(dictionaryPath))
        {
          val filePath = dictionaryFile.getPath
          if(fs.isFile(filePath) && filePath.getName.contains(dicFileTag) && dictionaryFile.getLen != 0)
          {
            val tmpRdd = sqlContext.sparkContext.textFile(filePath.toString)
              .map(x => {val tokens = x.split(","); (tokens(0).toInt, tokens(1))})
              .groupByKey()
              .map(x => (x._1, x._2.toArray))
            if(dictionaryRdd != null)
            {
              dictionaryRdd = dictionaryRdd.union(tmpRdd)
            }
            else
            {
              dictionaryRdd = tmpRdd
            }
          }
        }
      }

      distinctValues = dictionaryRdd
        .filter(x => dfColumns.contains(csvFileColumns(x._1)))
        .filter(x => dimColumns.contains(csvFileColumns(x._1)))
        .collect()
    }
    catch
    {
      case ex: Exception =>
        logError("read local dictionary failed")
        throw ex
    }
    finally
    {
      if(fs != null)
      {
        fs.close()
      }
    }

    val requireColumnsArr = new ArrayBuffer[String]()
    for(value <- distinctValues)
    {
      requireColumnsArr += csvFileColumns(value._1)
    }

    (distinctValues, requireColumnsArr.toArray)
  }

  /**
   * combine local dictionary with SQLContext and CarbonLoadModel
   *
   * @param sqlContext
   * @param model
   * @param distinctValues
   * @return inputRdd
   */
  private def ReadExistDictionaryAndCombine(sqlContext: SQLContext, model: DictionaryLoadModel,
      distinctValues: Array[(Int, Array[String])],csvColumnIndexMap: HashMap[String, Int]):RDD[(Int, HashSet[String])] =
  {
    val distinctValuesList = new ArrayBuffer[(Int, HashSet[String])]()
    //load exists dictionary file to list of HashMap
    val (dicts, existDicts) = GlobalDictionaryUtil.readGlobalDictionaryFromFile(model)
    // local combine set
    val sets = new Array[HashSet[String]](model.columns.length)
    // Map[csvColumnIndex -> requiredColumnIndex]
    val requireColumnIndexMap = new HashMap[Int, Int]()
    for(i <- 0 until model.columns.length)
    {
      sets(i) = new HashSet[String]
      distinctValuesList += ((i, sets(i)))
      val csvColumnIndex = csvColumnIndexMap.get(model.columns(i)).get
      requireColumnIndexMap.put(csvColumnIndex, i)
    }

    for(value <- distinctValues)
    {
      requireColumnIndexMap.get(value._1) match
      {
        case Some(index) =>
          if(existDicts(index))
          {
            for(record <- value._2)
            {
              if(!dicts(index).contains(record))
              {
                sets(index).add(record)
              }
            }
          }
          else
          {
            for(record <- value._2)
            {
              sets(index).add(record)
            }
          }
        case None =>
      }
    }

   // convert distinctValuesList to inputRDD
   sqlContext.sparkContext.makeRDD(distinctValuesList)
  }

  /**
   * generate global dictionary with SQLContext and CarbonLoadModel
   *
   * @param sqlContext
   * @param carbonLoadModel
   * @return a integer 1: successfully
   */
  def generateGlobalDictionary(sqlContext: SQLContext,
                               carbonLoadModel: CarbonLoadModel,
                               hdfsLocation: String,
                               isSharedDimension: Boolean,
                               localDictionaryPath: String) = {
    val rtn = 1
    try {
      val table = new CarbonTableIdentifier(carbonLoadModel.getSchemaName, carbonLoadModel.getTableName)

      //create dictionary folder if not exists
      val dictfolderPath = CarbonDictionaryUtil.getDirectoryPath(table, hdfsLocation, isSharedDimension)
      val created = CarbonUtil.checkAndCreateFolder(dictfolderPath)
      if (!created) {
        logError("Dictionary Folder creation status :: " + created)
        throw new IOException("Failed to created dictionary folder");
      }
      //load data by using dataSource com.databricks.spark.csv
      //need new SQLContext to use spark-csv
      var df = loadDataFrame(sqlContext, carbonLoadModel.getFactFilePath)

      // fill the map[columnIndex -> columnName]
      val csvFileColumns = df.columns
      val csvColumnIndexMap = new HashMap[String, Int]()
      for(i <- 0 until csvFileColumns.length)
      {
        csvColumnIndexMap.put(csvFileColumns(i), i)
      }

      //configuration for generating global dict from dimension table
      var dimensionSet = new HashSet[String]
      if (carbonLoadModel.getDimFolderPath != null) {
        dimensionSet = extractHierachyDimension(carbonLoadModel.getSchema)
        if (dimensionSet.size > 0) {
          var columnName = ""
          val setIter = dimensionSet.toIterator
          while (setIter.hasNext) {
            columnName = setIter.next
            df = df.drop(columnName)
          }
        }
      }
      //columns which need to generate global dictionary file
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      var requireColumns = pruneColumns(carbonTable.getDimensionByTableName(carbonTable.getFactTableName).toSeq.map(dim => dim.getColName).toArray, df.columns)

      var distinctValues: Array[(Int, Array[String])] = null
      if(localDictionaryPath != null)
      {
        val dimColumns = carbonTable.getDimensionByTableName(carbonTable.getFactTableName).toSeq.map(dim => dim.getColName).toList
        val dfColumns = df.columns.toList
       // TODO: get user config dictionary file name
        val dicFileTag = "Dictionary"
        // read local dictionary file, and prune columns according to the present column in CSV file, dimension columns, current df columns
        val result = ReadLocalDictionaryAndPruneColumns(sqlContext, csvFileColumns, dimColumns, dfColumns, localDictionaryPath, dicFileTag)
        distinctValues = result._1
        requireColumns = result._2
      }

      if (requireColumns.size >= 1) {
        //select column to push down pruning
        df = df.select(requireColumns.head, requireColumns.tail: _*)
        val model = createDictionaryLoadModel(table, requireColumns,
          hdfsLocation, dictfolderPath, isSharedDimension)
        var inputRDD: RDD[(Int, HashSet[String])] = null
        if(localDictionaryPath != null)
        {
          inputRDD = ReadExistDictionaryAndCombine(sqlContext, model, distinctValues, csvColumnIndexMap)
          .partitionBy(new ColumnPartitioner(requireColumns.length))
        }
        else
        {
          //combine distinct value in a block and partition by column
          inputRDD = new CarbonBlockDistinctValuesCombineRDD(df.rdd, model)
            .partitionBy(new ColumnPartitioner(requireColumns.length))
        }
        //generate global dictionary files
        val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, model).collect()
        //check result status
        checkStatus(statusList)
      } else {
        logInfo("have no column need to generate global dictionary")
      }
      // generate global dict from dimension file
      if (carbonLoadModel.getDimFolderPath != null) {
        val fileMapArray = carbonLoadModel.getDimFolderPath.split(",")
        for (fileMap <- fileMapArray) {
          val dimTableName = fileMap.split(":")(0)
          val dimFilePath = fileMap.substring(dimTableName.length + 1)
          var dimDataframe = loadDataFrame(sqlContext, dimFilePath)
          val requireColumnsforDim = pruneColumns(dimensionSet.toArray, dimDataframe.columns)
          if (requireColumnsforDim.size >= 1) {
            dimDataframe = dimDataframe.select(requireColumnsforDim.head, requireColumnsforDim.tail: _*)
            val modelforDim = createDictionaryLoadModel(table, requireColumnsforDim,
              hdfsLocation, dictfolderPath, isSharedDimension)
            val inputRDDforDim = new CarbonBlockDistinctValuesCombineRDD(dimDataframe.rdd, modelforDim)
              .partitionBy(new ColumnPartitioner(requireColumnsforDim.length))
            val statusListforDim = new CarbonGlobalDictionaryGenerateRDD(inputRDDforDim, modelforDim).collect()
            checkStatus(statusListforDim)
          } else {
            logInfo(s"No columns in dimension table $dimTableName to generate global dictionary")
          }
        }
      }
    } catch {
      case ex: Exception =>
        logError("generate global dictionary failed")
        throw ex
    }
    rtn
  }
}