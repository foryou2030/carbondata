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

package com.huawei.unibi.molap.engine.executer.impl.comparator;

import java.util.Comparator;
import java.util.List;

import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

/**
 * Class Description : Comparator responsible for Comparing to ByteArrayWrapper based on key
 * Version 1.0
 */
public class MaksedByteComparatorBAW implements Comparator<KeyValueHolder>
{
    /**
     * compareRange
     */
    private int[] index;
    
    /**
     * sortOrder
     */
    private byte sortOrder;
    
    /**
     * maskedKey
     */
    private byte[] maskedKey;
    /**
     * MaksedByteResultComparator Constructor
     */
    public MaksedByteComparatorBAW(int[] compareRange, byte sortOrder,byte[] maskedKey)
    {
        this.index=compareRange;
        this.sortOrder=sortOrder;
        this.maskedKey=maskedKey;
    }

    public MaksedByteComparatorBAW(byte sortOrder)
    {
        this.sortOrder=sortOrder;
    }

    /**
     * This method will be used to compare two byte array
     * @param o1
     * @param o2
     */
    @Override
    public int compare(KeyValueHolder byteArrayWrapper1, KeyValueHolder byteArrayWrapper2)
    {
        int cmp = 0;
        byte[] o1 = byteArrayWrapper1.key.getMaskedKey();
        byte[] o2 = byteArrayWrapper2.key.getMaskedKey();
        if(null!=index)
        {
        for(int i = 0;i < index.length;i++)
        {
            int a = (o1[index[i]] & this.maskedKey[i]) & 0xff;
            int b = (o2[index[i]] & this.maskedKey[i]) & 0xff;
            cmp = a - b;
            if(cmp != 0)
            {

                if(sortOrder == 1)
                {
                   return  cmp=cmp * -1;
                }
            }
        }
       }
        List<byte[]> listOfDirectSurrogateVal1= byteArrayWrapper1.key.getDirectSurrogateKeyList();
        List<byte[]> listOfDirectSurrogateVal2= byteArrayWrapper2.key.getDirectSurrogateKeyList();
        if(cmp == 0)
        {
            if(null != listOfDirectSurrogateVal1 && null!=listOfDirectSurrogateVal2)
            {
                for(int i = 0;i < listOfDirectSurrogateVal1.size();i++)
                {
                    cmp = UnsafeComparer.INSTANCE.compareTo(listOfDirectSurrogateVal1.get(i),
                            listOfDirectSurrogateVal2.get(i));
                    if(cmp != 0)
                    {

                        if(sortOrder == 1)
                        {
                            cmp=cmp * -1;
                        }
                        return cmp;
                    }
                }
            }
        }
        return cmp;
    }
}
