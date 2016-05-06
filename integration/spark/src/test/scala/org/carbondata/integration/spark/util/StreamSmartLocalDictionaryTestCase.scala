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

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.{CarbonHiveContext, QueryTest}
import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.integration.spark.load.{CarbonLoadModel, CarbonLoaderUtil}
import org.scalatest.BeforeAndAfterAll

/**
  * Test Case for org.carbondata.integration.spark.util.GlobalDictionaryUtil
  *
  * @date: Apr 10, 2016 10:34:58 PM
  * @See org.carbondata.integration.spark.util.GlobalDictionaryUtil
  */
class StreamSmartLocalDictionaryTestCase extends QueryTest with BeforeAndAfterAll {

  var pwd: String = _
  var streamSampleRelation: CarbonRelation = _
  var streamComplexRelation: CarbonRelation = _
  var sampleLocalDictionaryFile: String = _
  var complexLocalDictionaryFile: String = _

  def buildCarbonLoadModel(relation: CarbonRelation,
    filePath: String,
    dimensionFilePath: String,
    header: String,
    localDictFilePath: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    // carbonLoadModel.setSchema(relation.cubeMeta.schema)
    val table = relation.cubeMeta.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setDimFolderPath(dimensionFilePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(",")
    carbonLoadModel.setComplexDelimiterLevel1("\\$")
    carbonLoadModel.setComplexDelimiterLevel2("\\:")
    carbonLoadModel.setLocalDictPath(localDictFilePath)
    carbonLoadModel.setDictFileExt(".dictionary")
    carbonLoadModel
  }

  override def beforeAll {
    buildTestData
    // second time comment this line
    buildTable
    buildRelation
  }

  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    sampleLocalDictionaryFile = pwd + "/src/test/resources/localdictionary/sample/20160423/1400_1405/"
    complexLocalDictionaryFile = pwd + "/src/test/resources/localdictionary/complex/20160423/1400_1405/"
  }

  def buildTable() = {
    try {
      sql(
        "CREATE CUBE IF NOT EXISTS sample_stream DIMENSIONS (id STRING, name STRING, city STRING) " +
          "MEASURES (age INTEGER) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "create cube complextypes_stream dimensions(deviceInformationId integer, channelsId string, " +
          "ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, MAC " +
          "array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry string, " +
          "ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
          "string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) " +
          "measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = " +
          "'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ," +
          "COLUMNS= (deviceInformationId) , PARTITION_COUNT=1] )"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog
    streamSampleRelation = catalog.lookupRelation1(Option("default"), "sample_stream", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
    streamComplexRelation = catalog.lookupRelation1(Option("default"), "complextypes_stream", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
  }

  test("Support generate global dictionary from streamSmart local dictionary") {
    var header = "id,name,city,age"
    var carbonLoadModel = buildCarbonLoadModel(streamSampleRelation, null, null, header, sampleLocalDictionaryFile)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        streamSampleRelation.cubeMeta.dataPath)
  }

  test("Support generate global dictionary from streamSmart local dictionary file for complex type") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber"
    var carbonLoadModel = buildCarbonLoadModel(streamComplexRelation, null, null, header, complexLocalDictionaryFile)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
      carbonLoadModel,
      streamComplexRelation.cubeMeta.dataPath)
  }
}
