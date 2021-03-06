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

package com.huawei.datasight.molap.autoagg.model;

/**
 * DDL identifier that whether request is for data stats or query stats
 * @author A00902717
 *
 */
public enum Request
{

	DATA_STATS("DATA_STATS"),QUERY_STATS("QUERY_STATS");
	
	//aggregate suggestion type
	private String aggSuggType;
	
	Request(String aggSuggType)
	{
		this.aggSuggType=aggSuggType;
	}
	public static Request getRequest(String requestType)
	{
		if("DATA_STATS".equalsIgnoreCase(requestType))
		{
			return DATA_STATS;
		}
		else
		{
			return QUERY_STATS;
		}
	}
	
	public String getAggSuggestionType()
	{
		return this.aggSuggType;
	}
	
}
