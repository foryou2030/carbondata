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

package com.huawei.unibi.molap.query.metadata;

/**
 * Molap dimension level;
 */
public class MolapDimensionLevel extends AbstractMolapLevel
{
	private static final long serialVersionUID = 4012085091766940643L;	

	/**
	 * Dimension name
	 */
	private String dimensionName;
	
	/**
	 * Hierarchy name
	 */
	private String hierarchyName;
	
	
	/**
	 * level name
	 */
	private String levelName;
	
	
	/**
	 * Constructor
	 * @param dimensionName
	 * @param hierarchyName
	 * @param levelName
	 */
	public MolapDimensionLevel(String dimensionName, String hierarchyName,
			String levelName) 
	{
		this.dimensionName = dimensionName;
		this.hierarchyName = hierarchyName;
		this.levelName = levelName;
	}

	/**
	 * @return the dimensionName
	 */
	public String getDimensionName() 
	{
		return dimensionName;
	}

	/**
	 * @return the hierarchyName
	 */
	public String getHierarchyName() 
	{
		return hierarchyName;
	}

	/**
	 * @return the levelName
	 */
	public String getName() 
	{
		return levelName;
	}

	@Override
	public MolapLevelType getType() 
	{
		
		return MolapLevelType.DIMENSION;
	}
}
