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

package com.huawei.datasight.common.util;

import com.huawei.datasight.common.cubemeta.CubeMetadata;
import com.huawei.datasight.molap.load.MolapLoadModel;
import com.huawei.datasight.molap.load.MolapLoaderUtil;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapProperties;

public class LoadDataUtils {
	public static MolapLoadModel prepareLoadModel(String schemaName, String cubeName, 
			String uniqueId) throws Exception
	{
		String factFilePath = MolapProperties.getInstance().getProperty("molap.testdata.path") + 
				  "/data.csv";
		Cube cube = MolapMetadata.getInstance().getCube(schemaName+"_"+cubeName);
		if(cube == null)
		{
			throw new Exception("Cube "+cubeName +" of "+schemaName+" does not exist.");
		}
		CubeMetadata cubeMeta = CommonUtils.readCubeMetaDataFile(schemaName, cubeName);
		MolapLoadModel copy = new MolapLoadModel();
		copy.setCubeName(cubeName+'_'+uniqueId);
//		copy.setDimFolderPath(factFilePath);
		copy.setFactFilePath(factFilePath);
		copy.setSchemaName(schemaName+'_'+uniqueId);
		copy.setTableName(cube.getFactTableName());
		copy.setPartitionId(uniqueId);
		Schema schema = CommonUtils.createSchemaObjectFromXMLString(cubeMeta.getSchema());
		copy.setSchema(schema);
		if(uniqueId != null && schema!=null)
        {
            String originalSchemaName = schema.name;
            String originalCubeName = schema.cubes[0].name;
            schema.name = originalSchemaName + '_' + uniqueId;
            schema.cubes[0].name = originalCubeName + '_' + uniqueId;
        }
		return copy;
	}
	public static void loadCube(MolapLoadModel model, int currentRestructNumber) throws Exception
	{
		String storeLocation  = MolapProperties.getInstance().getProperty(
				MolapCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"));
		String hdfsStoreLocation  = MolapProperties.getInstance().getProperty(
				MolapCommonConstants.STORE_LOCATION_HDFS, null) +"/store";
		String kettleHomePath = MolapProperties.getInstance().getProperty("molap.kettle.home", null);
		
		MolapLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, 
				kettleHomePath, currentRestructNumber);
		MolapLoaderUtil.copyCurrentLoadToHDFS(model, 0, "Load_0", null, 0);
	}
}
