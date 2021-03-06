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

package com.huawei.unibi.molap.engine.columnar.keyvalue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;



public class NonFilterScanResult extends AbstractColumnarScanResult
{
    public NonFilterScanResult(int keySize,int[] selectedDimensionIndex)
    {
        super(keySize,selectedDimensionIndex);
    }

    public  double getDoubleValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableDoubleValueByIndex(currentRow);
    }

    public  BigDecimal getBigDecimalValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableBigDecimalValueByIndex(currentRow);
    }

    public  long getLongValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableLongValueByIndex(currentRow);
    }

    public byte[] getByteArrayValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableByteArrayValueByIndex(currentRow);
    }
    
    public byte[] getKeyArray(ByteArrayWrapper keyVal)
    {
        ++currentRow;
        return getKeyArray(++sourcePosition,keyVal);
    }

    public List<byte[]> getKeyArrayWithComplexTypes(Map<Integer, GenericQueryType> complexQueryDims,ByteArrayWrapper keyVal)
    {
        ++currentRow;
        return getKeyArrayWithComplexTypes(++sourcePosition, complexQueryDims,keyVal);
    }
    
    
    
    public int getDimDataForAgg(int dimOrdinal)
    {
        return getSurrogateKey(currentRow, dimOrdinal);
    }
    @Override
    public byte[] getKeyArray()
    {
        ++currentRow;
        return getKeyArray(++sourcePosition,null);
    }
    @Override
    public byte[] getHighCardinalityDimDataForAgg(int dimOrdinal)
    {
        
            ColumnarKeyStoreMetadata columnarKeyStoreMetadata = columnarKeyStoreDataHolder[dimOrdinal]
                    .getColumnarKeyStoreMetadata();
            if(null != columnarKeyStoreMetadata.getMapOfColumnarKeyBlockDataForDirectSurroagtes())
            {
                Map<Integer, byte[]> mapOfDirectSurrogates = columnarKeyStoreMetadata
                        .getMapOfColumnarKeyBlockDataForDirectSurroagtes();
                if(null==columnarKeyStoreMetadata.getColumnReverseIndex())
                {
                    return mapOfDirectSurrogates.get(currentRow);
                }
                return mapOfDirectSurrogates.get(columnarKeyStoreMetadata.getColumnReverseIndex()[currentRow]);
            }
            return null;
            // TODO Auto-generated method stub

        

    }
   
    

    @Override
    public void getComplexDimDataForAgg(GenericQueryType complexType, DataOutputStream dataOutputStream)
            throws IOException
    {
        getComplexSurrogateKey(currentRow, complexType, dataOutputStream);
    }
	
	public  int getRowIndex()
    {
        return currentRow;
    }
}
