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

package com.huawei.unibi.molap.engine.filter.executor.impl;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.filter.executor.FilterExecutor;
import com.huawei.unibi.molap.util.ByteUtil;

public class MergeSortBasedNonUniqueBlockNotEquals implements FilterExecutor
{
    @Override
    public BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filterValues)
    {
        return setFilterdIndexToBitSetSortedBased(keyBlockArray, numerOfRows, filterValues);
    }

    private BitSet setFilterdIndexToBitSetSortedBased(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows,
            byte[][] filterValues)
    {
        BitSet bitSet = new BitSet(numerOfRows);
        bitSet.flip(0,numerOfRows);
        int filterCounter = 0;
        int rowCounter = 0;
        int[] columnIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        while(filterCounter < filterValues.length && rowCounter < numerOfRows)
        {
            if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(keyBlockArray.getKeyBlockData(), rowCounter
                    * filterValues[filterCounter].length, filterValues[filterCounter].length,
                    filterValues[filterCounter], 0, filterValues[filterCounter].length) == 0)
            {
                if(columnIndex != null)
                {
                    bitSet.flip(columnIndex[rowCounter]);
                }
                else
                {
                    bitSet.flip(rowCounter);
                }
                rowCounter++;
            }
            else
            {
                filterCounter++;
            }
        }
        return bitSet;
    }

}
