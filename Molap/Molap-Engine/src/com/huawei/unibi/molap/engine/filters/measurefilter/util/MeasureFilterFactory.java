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

package com.huawei.unibi.molap.engine.filters.measurefilter.util;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcExpressionResolverUtil;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.filters.measurefilter.AndMeasureGroupFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.EqualsMeasureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.GreaterThanMeaureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.GreaterThanOrEqualMeaureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.GroupMeasureFilterModel;
import com.huawei.unibi.molap.engine.filters.measurefilter.LessThanMeasureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.LessThanOrEqualToMeasureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.MeasureFilter;
import com.huawei.unibi.molap.engine.filters.measurefilter.MeasureFilterModel;
import com.huawei.unibi.molap.engine.filters.measurefilter.NotEmptyMeasureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.NotEqualsMeasureFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.OrMeasureGroupFilterImpl;
import com.huawei.unibi.molap.engine.filters.measurefilter.GroupMeasureFilterModel.MeasureFilterGroupType;
import com.huawei.unibi.molap.engine.filters.measurefilter.MeasureFilterModel.MeasureFilterType;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

public final class MeasureFilterFactory
{
    private MeasureFilterFactory()
    {
        
    }
    /**
     * Get the measue filter as per the filter type.
     * @param filterType
     * @param filterValue
     * @return
     */
    public static MeasureFilter getMeasureFilter(MeasureFilterType filterType, double filterValue,int index,MolapCalcFunction calcFunction)
    {
        switch(filterType)
        {
            case EQUAL_TO:
                return new EqualsMeasureFilterImpl(filterValue,index,calcFunction);
            case NOT_EQUAL_TO:
                return new NotEqualsMeasureFilterImpl(filterValue,index,calcFunction);
            case GREATER_THAN:
                return new GreaterThanMeaureFilterImpl(filterValue,index,calcFunction);
            case LESS_THAN:
                return new LessThanMeasureFilterImpl(filterValue,index,calcFunction);
            case GREATER_THAN_EQUAL:
                return new GreaterThanOrEqualMeaureFilterImpl(filterValue,index,calcFunction);
            case LESS_THAN_EQUAL:
                return new LessThanOrEqualToMeasureFilterImpl(filterValue,index,calcFunction);
            case NOT_EMPTY:
                return new NotEmptyMeasureFilterImpl(index);
            default:
                return null;
        }
    }
    
    /**
     * Get all the measure filter instances as per the passed filters.
     * @param filters
     * @return
     */
    public static MeasureFilter[] getMeasureFilter(MeasureFilterModel[] filters,int index,List<Measure> queryMsrs)
    {
        
        MeasureFilter[] msrfilters = new MeasureFilter[filters.length];
        
        int i = 0;
        for(MeasureFilterModel measureFilter : filters)
        {
            if(measureFilter != null)
            {
                MolapCalcFunction calcFunction = null;
                if(measureFilter.getExp() != null)
                {
                    calcFunction = MolapCalcExpressionResolverUtil.createCalcExpressions(measureFilter.getExp(), queryMsrs);
                }
                msrfilters[i] = getMeasureFilter(measureFilter.getFilterType(), measureFilter.getFilterValue(),index,calcFunction);
            }
            i++;
        }
        return msrfilters;
    }
    
    
    /**
     * Get all the measure filter instances as per the passed filters.
     * @param filters
     * @return
     */
    public static MeasureFilter[][] getMeasureFilter(MeasureFilterModel[][] filters,List<Measure> queryMsrs)
    {
        
        MeasureFilter[][] msrfilters = new MeasureFilter[filters.length][];
        
        int i = 0;
        for(MeasureFilterModel[] measureFilter : filters)
        {
            if(measureFilter != null)
            {
                msrfilters[i] = getMeasureFilter(measureFilter,i,queryMsrs);
            }
            i++;
        }
        return msrfilters;
    }
    
    /**
     * Below method will be used to get the Measure Filter
     * @param msrConstraints
     * @return measure filters
     */
    public static MeasureFilter[] getFilterMeasures(GroupMeasureFilterModel[] msrConstraints,List<Measure> queryMsrs)
    {
        if(msrConstraints == null)
        {
            return null;
        }
        List<MeasureFilter> measureFilters = new ArrayList<MeasureFilter>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(int i = 0;i < msrConstraints.length;i++)
        {
            if(msrConstraints[i] != null)
            {
                if(msrConstraints[i].getFilterGroupType().equals(MeasureFilterGroupType.OR))
                {
                    OrMeasureGroupFilterImpl  groupFilter = new OrMeasureGroupFilterImpl(getMeasureFilter(msrConstraints[i].getFilterModels(),queryMsrs));
                    if(groupFilter.isMsrFilterEnabled())
                    {
                        measureFilters.add(groupFilter);
                    }
                }
                else
                {
                    AndMeasureGroupFilterImpl  andGroupFilter = new AndMeasureGroupFilterImpl(getMeasureFilter(msrConstraints[i].getFilterModels(),queryMsrs));
                    if(andGroupFilter.isMsrFilterEnabled())
                    {
                        measureFilters.add(andGroupFilter);
                    }
                }
            }
        }
        return measureFilters.toArray(new MeasureFilter[measureFilters.size()]);
    }

}
