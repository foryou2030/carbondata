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

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;

/**
 * This class will have information about level metadata
 * @author A00902717
 *
 */
public class LevelMetaInfo
{
	
	
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(LevelMetaInfo.class.getName());
    
    private int[] dimCardinality;
    
	public LevelMetaInfo(MolapFile file,String tableName)
	{
		initialise(file,tableName);
	}
	private void initialise(MolapFile file,final String tableName)
	{
		
		if (file.isDirectory())
		{
			MolapFile[] files = file.listFiles(new MolapFileFilter()
			{
				public boolean accept(MolapFile pathname)
				{
					return (!pathname.isDirectory())
							&& pathname
									.getName()
									.startsWith(
											MolapCommonConstants.LEVEL_METADATA_FILE)
							&& pathname.getName().endsWith(
									tableName + ".metadata");
				}

			});
			try
			{
				dimCardinality = MolapUtil
						.getCardinalityFromLevelMetadataFile(files[0]
								.getAbsolutePath());
			}
			catch (MolapUtilException e)
			{
				LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
			}
		}
		
	}
	public int[] getDimCardinality()
	{
		return dimCardinality;
	}
	
	

}
