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

package com.huawei.unibi.molap.engine.aggregator.impl;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.engine.datastorage.Member;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * AbstractMeasureAggregator 
 * Used for custom Molap Aggregator
 *
 */
public abstract class AbstractMeasureAggregator implements MeasureAggregator//,ICustomRolapAggregator
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * 
     */
    private KeyGenerator generator;
   
    private String cubeUniqueName;
    
    private String schemaName;
    
    private String cubeName;
    
    private CustomAggregatorHelper aggregatorHelper;
    
    /**
     * isDataLoadRequest
     */
    private boolean isDataLoadRequest;
    
    public AbstractMeasureAggregator()
    {
        
    }

    public AbstractMeasureAggregator(KeyGenerator generator, String cubeUniqueName)
    {
        this.generator = generator;
        this.cubeUniqueName  = cubeUniqueName;
    }
    
    
    /**
     * @param key
     * @param offset
     * @param length
     * @param tableName
     * @param columnName
     * @param dimensionName
     * @param hierarchyName
     * @param keyOrdinal
     * @return
     */
    public String getDimValue(byte[] key, int offset, int length,String tableName,String columnName,String dimensionName, String hierarchyName, int keyOrdinal)
    {
        byte[] val = new byte[length];
        System.arraycopy(key, offset, val, 0, length);
        long[] ls = generator.getKeyArray(val);// CHECKSTYLE:OFF Approval
        if(!isDataLoadRequest)
        {
                                                   // No:Approval-280
            Member memberByID = null;// slice.getMemberCache(columnName)
            List<InMemoryCube> slices = InMemoryCubeStore.getInstance().getActiveSlices(cubeUniqueName);
            for(InMemoryCube slic : slices)
            {
                Member member = slic.getMemberCache(
                        tableName + '_' + columnName + "_" + dimensionName + "_" + hierarchyName).getMemberByID(
                        (int)ls[keyOrdinal]);
                if(member != null)
                {
                    memberByID = member;
                    break;
                }
            }
            if(memberByID == null)
            {
                return "-";
            }
            return memberByID.toString();// CHECKSTYLE:ON
        }
        else
        {
          //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_013
            return aggregatorHelper.getDimValue(tableName, columnName,(int) ls[keyOrdinal], cubeName, schemaName);//CHECKSTYLE:ON
        }
    }
     
    @Override
    public void agg(double arg0)
    {

    }
    
    @Override
    public void setNewValue(Object arg0)
    {

    }
    
    @Override
    public MeasureAggregator getCopy()
    {
        // TODO Auto-generated method stub
        return null;
    }
   
    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @return the generator
     */
    public KeyGenerator getGenerator()
    {
        return generator;
    }

    /**
     * @param generator the generator to set
     */
    public void setGenerator(KeyGenerator generator)
    {
        this.generator = generator;
    }

    /**
     * @return the isDataLoadRequest
     */
    public boolean isDataLoadRequest()
    {
        return isDataLoadRequest;
    }

    /**
     * @param isDataLoadRequest the isDataLoadRequest to set
     */
    public void setDataLoadRequest(boolean isDataLoadRequest)
    {
        this.isDataLoadRequest = isDataLoadRequest;
    }

    /**
     * @return the aggregatorHelper
     */
    public CustomAggregatorHelper getAggregatorHelper()
    {
        return aggregatorHelper;
    }

    /**
     * @param aggregatorHelper the aggregatorHelper to set
     */
    public void setAggregatorHelper(CustomAggregatorHelper aggregatorHelper)
    {
        this.aggregatorHelper = aggregatorHelper;
    }
}
