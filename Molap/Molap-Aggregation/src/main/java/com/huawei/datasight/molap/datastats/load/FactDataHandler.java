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

package com.huawei.datasight.molap.datastats.load;

import java.util.ArrayList;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * This class will read given fact file
 * 
 * @author A00902717
 *
 */
public class FactDataHandler
{

	/**
	 * Attribute for Molap LOGGER
	 */
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(FactDataHandler.class.getName());

	private ValueCompressionModel compressionModel;

	private LevelMetaInfo levelMetaInfo;

	private boolean[] aggKeyBlock;

	private int[] keyBlockSize;

	private boolean isFileStore;

	private Cube metaCube;

	private String tableName;

	private FileHolder fileHolder;

	private int[] dimensionCardinality;
	
	private int keySize;
	
	private List<DataInputStream> streams;
	
	private List<FactDataNode> factDataNodes;

	public FactDataHandler(Cube metaCube, LevelMetaInfo levelMetaInfo, String tableName, int keySize, List<DataInputStream> streams)
	{
		this.metaCube = metaCube;
		this.levelMetaInfo = levelMetaInfo;
		this.tableName = tableName;
		this.keySize=keySize;
		this.streams=streams;
		initialise();

	}

	private void initialise()
	{
		// Initializing dimension cardinality
		dimensionCardinality = levelMetaInfo.getDimCardinality();
		aggKeyBlock = new boolean[dimensionCardinality.length];
		
		boolean isAggKeyBlock = Boolean.parseBoolean(MolapCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE);
        if(isAggKeyBlock)
        {
            int highCardinalityValue = Integer.parseInt(MolapProperties.getInstance().getProperty(
                    MolapCommonConstants.HIGH_CARDINALITY_VALUE,
                    MolapCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
            for(int i = 0;i < dimensionCardinality.length;i++)
            {
                if(dimensionCardinality[i] < highCardinalityValue)
                {
                    aggKeyBlock[i] = true;
                }
            }
        }
		

		// Initializing keyBlockSize
		int dimSet = Integer
				.parseInt(MolapCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
		keyBlockSize = new MultiDimKeyVarLengthEquiSplitGenerator(
				MolapUtil.getIncrementedCardinalityFullyFilled(dimensionCardinality
						.clone()), (byte) dimSet).getBlockKeySize();

		// Initializing isFileStore
		initializeFileStore();

		// Initializing fileHolder
		fileHolder = FileFactory.getFileHolder(FileFactory.getFileType());
	}

	private void initializeFileStore()
	{
		String schemaAndcubeName = metaCube.getCubeName();
		String schemaName = metaCube.getSchemaName();
		String cubeName = schemaAndcubeName.substring(
				schemaAndcubeName.indexOf(schemaName + '_')
						+ schemaName.length() + 1, schemaAndcubeName.length());
		String modeValue = metaCube.getMode();
		if (modeValue
				.equalsIgnoreCase(MolapCommonConstants.MOLAP_MODE_DEFAULT_VAL))
		{
			isFileStore = true;
		}

		if (!isFileStore)
		{
			boolean parseBoolean = Boolean
					.parseBoolean(MolapProperties
							.getInstance()
							.getProperty(
									MolapCommonConstants.MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY,
									MolapCommonConstants.MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY_DEFAULTVALUE));
			if (!parseBoolean && tableName.equals(metaCube.getFactTableName()))
			{
				LOGGER.info(
						MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
						"Mode set for cube "
								+ schemaName
								+ ':'
								+ cubeName
								+ "as mode="
								+ modeValue
								+ ": but as "
								+ MolapCommonConstants.MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY
								+ " is false it will be file mode");
				isFileStore = true;
			}
		}
	}

	/**
	 * This method reads given fact stream
	 * 
	 * @param sources
	 */
	public FactDataReader getFactDataReader()
	{
		compressionModel = streams.get(0).getValueCompressionMode();
		long st = System.currentTimeMillis();
		
		factDataNodes= new ArrayList<FactDataNode>(streams.size());
		for (DataInputStream factStream : streams)
		{
			List<LeafNodeInfoColumnar> leafNodeInfoList = factStream
					.getLeafNodeInfoColumnar();
			// Coverity fix added null check
			if (null != leafNodeInfoList)
			{
				if (leafNodeInfoList.size() > 0)
				{
					leafNodeInfoList.get(0).getFileName();
					LOGGER.info(
							MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
							"Processing : "
									+ (leafNodeInfoList.get(0).getFileName())
									+ " : " + (System.currentTimeMillis() - st));
					st = System.currentTimeMillis();

				}
				for (LeafNodeInfoColumnar leafNodeInfo : leafNodeInfoList)
				{
					leafNodeInfo.setAggKeyBlock(aggKeyBlock);
					
					FactDataNode factDataNode = new FactDataNode(
							leafNodeInfo.getNumberOfKeys(), keyBlockSize,
							isFileStore, fileHolder, leafNodeInfo,
							compressionModel);
					factDataNodes.add(factDataNode);
					
					
				}
			}
		}
		return new FactDataReader(factDataNodes,keySize,fileHolder); 
		
	}
	

	/**
	 * This method returns cardinality of given dimension
	 * @param dimensionOrdinal
	 * @return
	 */
	public int getDimensionCardinality(int dimensionOrdinal)
	{
		return dimensionCardinality[dimensionOrdinal];
	}
	
}
