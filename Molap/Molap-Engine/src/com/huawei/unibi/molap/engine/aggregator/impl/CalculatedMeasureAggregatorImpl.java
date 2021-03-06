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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.engine.aggregator.CalculatedMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;

public class CalculatedMeasureAggregatorImpl implements CalculatedMeasureAggregator
{

    /**
     * 
     */
    private static final long serialVersionUID = -3735752250785327377L;
    
    /**
     * 
     */
    private MolapCalcFunction function;
    
    private double val;
    
    
    public CalculatedMeasureAggregatorImpl(MolapCalcFunction function)
    {
        this.function = function;
    }
    
    public CalculatedMeasureAggregatorImpl()
    {
    }

    @Override
    public void agg(double newVal)
    {
        
    }

    @Override
    public void agg(MolapReadDataHolder newVal, int index)
    {
       
    }
    
    @Override
    public void agg(Object newVal)
    {

    }

    @Override
    public byte[] getByteArray()
    {
        return null;
    }

    @Override
    public Double getDoubleValue()
    {
        return val;
    }

    @Override
    public Long getLongValue()
    {
        return (long)val;
    }

    @Override
    public BigDecimal getBigDecimalValue()
    {
        return new BigDecimal(val);
    }

    @Override
    public Object getValueObject()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void merge(MeasureAggregator aggregator)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setNewValue(Object newValue)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isFirstTime()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        output.writeDouble(val);
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        val = inPut.readDouble();
    }

    @Override
    public void calculateCalcMeasure(MeasureAggregator[] aggregators)
    {
        val = function.calculate(aggregators);
    }

    @Override
    public MeasureAggregator getCopy()
    {
        return this;
    }
    //we are not comparing the values
  /*  
    public boolean equals(MeasureAggregator msrAggregator){
        return compareTo(msrAggregator)==0;
    }
    */
    
    @Override
    public int compareTo(MeasureAggregator msrObj) 
    {
        double msrVal1 = getDoubleValue();
        double otherMsrVal1 = msrObj.getDoubleValue();
        if(msrVal1 > otherMsrVal1)
        {
            return 1;
        }
        if(msrVal1 < otherMsrVal1)
        {
            return -1;
        }
        return 0;
    }
    
    @Override
    public MeasureAggregator get()
    {
        return this;
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
        
    }

}
