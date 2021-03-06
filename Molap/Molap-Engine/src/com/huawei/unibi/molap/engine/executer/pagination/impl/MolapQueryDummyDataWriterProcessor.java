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

package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * This class is used in case pagination is enabled. It just collects the data and form one map.
 *
 */
public class MolapQueryDummyDataWriterProcessor implements DataProcessor
{

    private Map<ByteArrayWrapper, MeasureAggregator[]> map = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>();
    
    private int limit = -1;
    
    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {
        limit = model.getLimit();
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
        if(limit == -1 || map.size() < limit)
        {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key);
            map.put(arrayWrapper, measures);
        }
    }

    @Override
    public void finish() throws MolapPaginationException
    {

        
    }
    
    public  Map<ByteArrayWrapper, MeasureAggregator[]> getData()
    {
        return map;
    }

    /**
     * processGroup
     */
    @Override
    public void processGroup(GroupByHolder groupByHolder)
    {
        // No need to implement any thing
        
    }
}
