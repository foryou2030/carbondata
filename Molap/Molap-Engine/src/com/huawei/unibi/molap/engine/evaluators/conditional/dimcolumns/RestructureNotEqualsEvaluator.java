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

package com.huawei.unibi.molap.engine.evaluators.conditional.dimcolumns;

import java.util.BitSet;

import com.huawei.unibi.molap.engine.evaluators.AbastractRSConditionalEvalutor;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterProcessorPlaceHolder;
import com.huawei.unibi.molap.engine.expression.Expression;

public class RestructureNotEqualsEvaluator extends AbastractRSConditionalEvalutor
{

    public RestructureNotEqualsEvaluator(Expression exp, String defaultValue, int surrogate,boolean isExpressionResolve)
    {
        super(exp, defaultValue,surrogate,isExpressionResolve,false);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder)
    {
        BitSet bitSet = new BitSet(blockDataHolder.getLeafDataBlock().getnKeys());
        bitSet.flip(0, blockDataHolder.getLeafDataBlock().getnKeys());
        byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
        if(null!=filterValues && filterValues.length>0)
        {
            bitSet.flip(0, blockDataHolder.getLeafDataBlock().getnKeys());
        }
        return bitSet;
    }

    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue)
    {
        BitSet bitSet = new BitSet(1);
        bitSet.flip(0,1);
        return bitSet;
    }
}
