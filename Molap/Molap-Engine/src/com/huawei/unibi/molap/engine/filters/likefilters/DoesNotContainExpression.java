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

package com.huawei.unibi.molap.engine.filters.likefilters;

import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class DoesNotContainExpression extends MolapFilterInfo  implements FilterLikeExpressionIntf {



    /**
     * 
     */
    private static final long serialVersionUID = -8681863860748250016L;
    private LikeExpression likeContainsExpression;



    @Override
    public void setLikeExpression(LikeExpression expressionName)
    {
        likeContainsExpression=expressionName;

    }

    @Override
    public LikeExpression getLikeExpression()
    {
        // TODO Auto-generated method stub
        return likeContainsExpression;
    }

    @Override
    public void processLikeExpressionFilters(List<String> listFilterExpression, List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo, boolean hasNameColumn,
            Locale locale)
    {
        // TODO Auto-generated method stub
        
    }




}
