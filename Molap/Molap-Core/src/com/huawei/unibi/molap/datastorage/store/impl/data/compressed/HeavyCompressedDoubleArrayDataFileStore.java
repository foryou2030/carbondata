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

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.datastorage.store.impl.CompressedDataMeasureDataWrapper;

public class HeavyCompressedDoubleArrayDataFileStore
        extends AbstractHeavyCompressedDoubleArrayDataStore {
    /**
     * measuresOffsetsArray.
     */
    private long[] measuresOffsetsArray;

    /**
     * measuresLengthArray.
     */
    private int[] measuresLengthArray;

    /**
     * fileName.
     */
    private String fileName;

    /**
     * HeavyCompressedDoubleArrayDataFileStore.
     *
     * @param compressionModel
     * @param measuresOffsetsArray
     * @param measuresLengthArray
     * @param fileName
     */
    public HeavyCompressedDoubleArrayDataFileStore(ValueCompressionModel compressionModel,
            long[] measuresOffsetsArray, int[] measuresLengthArray, String fileName) {
        super(compressionModel);
        if (null != compressionModel) {
            this.fileName = fileName;
            this.measuresLengthArray = measuresLengthArray;
            this.measuresOffsetsArray = measuresOffsetsArray;
            for (int i = 0; i < values.length; i++) {
                values[i] =
                        compressionModel.getUnCompressValues()[i].getNew().getCompressorObject();
            }
        }
    }

    public HeavyCompressedDoubleArrayDataFileStore(ValueCompressionModel compressionModel) {
        super(compressionModel);
    }

    @Override public MeasureDataWrapper getBackData(int[] cols, FileHolder fileHolder) {
        if (null == compressionModel) {
            return null;
        }
        MolapReadDataHolder[] vals = new MolapReadDataHolder[values.length];

        if (cols != null) {
            for (int i = 0; i < cols.length; i++) {
                UnCompressValue copy = values[cols[i]].getNew();
                copy.setValue(fileHolder.readByteArray(fileName, measuresOffsetsArray[cols[i]],
                        measuresLengthArray[cols[i]]));
                vals[cols[i]] = copy.
                        uncompress(compressionModel.getChangedDataType()[cols[i]])
                        .getValues(compressionModel.getDecimal()[cols[i]],
                                compressionModel.getMaxValue()[cols[i]]);
                copy = null;
            }
        } else {
            for (int j = 0; j < vals.length; j++) {
                UnCompressValue copy = values[j].getNew();
                copy.setValue(fileHolder
                        .readByteArray(fileName, measuresOffsetsArray[j], measuresLengthArray[j]));
                vals[j] = copy.uncompress(compressionModel.getChangedDataType()[j])
                        .getValues(compressionModel.getDecimal()[j],
                                compressionModel.getMaxValue()[j]);
                copy = null;
            }
        }
        return new CompressedDataMeasureDataWrapper(vals);

    }

    @Override public MeasureDataWrapper getBackData(int cols, FileHolder fileHolder) {
        if (null == compressionModel) {
            return null;
        }
        MolapReadDataHolder[] vals = new MolapReadDataHolder[values.length];
        UnCompressValue copy = values[cols].getNew();
        copy.setValue(fileHolder
                .readByteArray(fileName, measuresOffsetsArray[cols], measuresLengthArray[cols]));
        vals[cols] = copy.uncompress(compressionModel.getChangedDataType()[cols])
                .getValues(compressionModel.getDecimal()[cols],
                        compressionModel.getMaxValue()[cols]);
        return new CompressedDataMeasureDataWrapper(vals);
    }

}
