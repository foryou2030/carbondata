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

package com.huawei.unibi.molap.engine.filter.executor;

import com.huawei.unibi.molap.engine.filter.executor.impl.MergeSortBasedNonUniqueBlockEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.NonUniqueBlockEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.NonUniqueBlockNotEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.UniqueBlockEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.UniqueBlockNotEquals;

public final class FilterExecuterFactory
{
    private FilterExecuterFactory()
    {
        
    }
    
    public static FilterExecutor getFilterExecuter(FilterExcutorType filterExcuterType)
    {
        switch(filterExcuterType)
        {
            case UNIQUE_EQUALS:
                return new UniqueBlockEquals();
            case UNIQUE_NOT_EQUALS:
                return new UniqueBlockNotEquals();
            case EQUALS:
                return new NonUniqueBlockEquals();
            case NOT_EQUALS:
                return new NonUniqueBlockNotEquals();
            case MERGESORT_EQUALS:
                return new MergeSortBasedNonUniqueBlockEquals();
            case MERGESORT_NOT_EQUALS:
                return new MergeSortBasedNonUniqueBlockEquals();
            default:
                return null;
        }
    }
}
