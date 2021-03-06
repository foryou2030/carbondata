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

package com.huawei.unibi.molap.datastorage.store.impl.data.compressed;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapWriteDataHolder;
import com.huawei.unibi.molap.util.ValueCompressionUtil;

public abstract class AbstractHeavyCompressedDoubleArrayDataStore
        implements NodeMeasureDataStore //NodeMeasureDataStore<double[]>
{

    /**
     * values.
     */
    protected UnCompressValue[] values;

    /**
     * compressionModel.
     */
    protected ValueCompressionModel compressionModel;

    /**
     * type
     */
    private char[] type;

    /**
     * AbstractHeavyCompressedDoubleArrayDataStore constructor.
     *
     * @param compressionModel
     */
    public AbstractHeavyCompressedDoubleArrayDataStore(ValueCompressionModel compressionModel) {
        this.compressionModel = compressionModel;
        if (null != compressionModel) {
            this.type = compressionModel.getType();
            values = new UnCompressValue[compressionModel.getUnCompressValues().length];
        }
    }

    @Override public byte[][] getWritableMeasureDataArray(MolapWriteDataHolder[] dataHolder) {
        for (int i = 0; i < compressionModel.getUnCompressValues().length; i++) {
            values[i] = compressionModel.getUnCompressValues()[i].getNew();
            if (type[i] != MolapCommonConstants.BYTE_VALUE_MEASURE
                    && type[i] != MolapCommonConstants.BIG_DECIMAL_MEASURE) {
                if (type[i] == MolapCommonConstants.BIG_INT_MEASURE) {
                    values[i].setValue(ValueCompressionUtil
                            .getCompressedValues(compressionModel.getCompType()[i],
                                    dataHolder[i].getWritableLongValues(),
                                    compressionModel.getChangedDataType()[i],
                                    (long) compressionModel.getMaxValue()[i],
                                    compressionModel.getDecimal()[i]));
                } else {
                values[i].setValue(ValueCompressionUtil
                        .getCompressedValues(compressionModel.getCompType()[i],
                                dataHolder[i].getWritableDoubleValues(),
                                compressionModel.getChangedDataType()[i],
                                    (double) compressionModel.getMaxValue()[i],
                                compressionModel.getDecimal()[i]));
                }
            } else {
                values[i].setValue(dataHolder[i].getWritableByteArrayValues());
            }
            values[i] = values[i].compress();
        }
        byte[][] returnValue = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            returnValue[i] = values[i].getBackArrayData();
        }
        return returnValue;
    }

    @Override public short getLength() {
        return values != null ? (short) values.length : 0;
    }

}
