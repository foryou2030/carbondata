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

package com.huawei.unibi.molap.engine.executer;

import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.iterator.MolapIterator;

public interface QueryExecutor
{
    
    /**
     * Below method will be used to execute the query 
     * 
     * @param queryModel
     *          query model, properties which will be required to execute the query 
     *          
     * @throws QueryExecutionException
     *          will throw query execution exception in case of any abnormal scenario 
     */
    MolapIterator<RowResult> execute(MolapQueryExecutorModel queryModel) throws QueryExecutionException;
    
    /**
     * Below method will be used to execute the query of QuickFilter 
     * 
     * @param queryModel
     *          , properties which will be required to execute the query 
     *          
     * @throws QueryExecutionException
     *          will throw query execution exception in case of any abnormal scenario 
     */    
    MolapIterator<RowResult> executeDimension(MolapQueryExecutorModel queryModel) throws QueryExecutionException;

}
