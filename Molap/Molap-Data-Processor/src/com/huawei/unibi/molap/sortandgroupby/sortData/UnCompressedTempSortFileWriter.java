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

package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.RemoveDictionaryUtil;

public class UnCompressedTempSortFileWriter extends AbstractTempSortFileWriter {

    /**
     * UnCompressedTempSortFileWriter
     *
     * @param writeBufferSize
     * @param dimensionCount
     * @param measureCount
     */
    public UnCompressedTempSortFileWriter(int dimensionCount, int complexDimensionCount,
            int measureCount, int highCardinalityCount, int writeBufferSize) {
        super(dimensionCount, complexDimensionCount, measureCount, highCardinalityCount,
                writeBufferSize);
    }

    public static void writeDataOutputStream(Object[][] records, DataOutputStream dataOutputStream,
            int measureCount, int dimensionCount, int highCardinalityCount,
            int complexDimensionCount) throws IOException {
        Object[] row;
        for (int recordIndex = 0; recordIndex < records.length; recordIndex++) {
            row = records[recordIndex];
            int fieldIndex = 0;

            for (int counter = 0; counter < dimensionCount; counter++) {
                dataOutputStream
                        .writeInt((Integer) RemoveDictionaryUtil.getDimension(fieldIndex++, row));
            }

            //write byte[] of high card dims
            if (highCardinalityCount > 0) {
                dataOutputStream.write(RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row));
            }
            fieldIndex = 0;
            for (int counter = 0; counter < complexDimensionCount; counter++) {
                int complexByteArrayLength = ((byte[]) row[fieldIndex]).length;
                dataOutputStream.writeInt(complexByteArrayLength);
                dataOutputStream.write(((byte[]) row[fieldIndex++]));
            }

            for (int counter = 0; counter < measureCount; counter++) {
                if (null != row[fieldIndex]) {
                    dataOutputStream.write((byte) 1);
                    dataOutputStream
                            .writeDouble((Double) RemoveDictionaryUtil.getMeasure(fieldIndex, row));
                } else {
                    dataOutputStream.write((byte) 0);
                }

                fieldIndex++;
            }

        }
    }

    /**
     * Below method will be used to write the sort temp file
     *
     * @param records
     */
    public void writeSortTempFile(Object[][] records) throws MolapSortKeyAndGroupByException {
        ByteArrayOutputStream blockDataArray = null;
        DataOutputStream dataOutputStream = null;
        int totalSize = 0;
        int recordSize = 0;
        try {
            recordSize = (measureCount * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE) + (dimensionCount
                    * MolapCommonConstants.INT_SIZE_IN_BYTE);
            totalSize = records.length * recordSize;

            blockDataArray = new ByteArrayOutputStream(totalSize);
            dataOutputStream = new DataOutputStream(blockDataArray);

            writeDataOutputStream(records, dataOutputStream, measureCount, dimensionCount,
                    highCardinalityCount, complexDimensionCount);
            stream.writeInt(records.length);
            byte[] byteArray = blockDataArray.toByteArray();
            stream.writeInt(byteArray.length);
            stream.write(byteArray);

        } catch (IOException e) {
            throw new MolapSortKeyAndGroupByException(e);
        } finally {
            MolapUtil.closeStreams(blockDataArray);
            MolapUtil.closeStreams(dataOutputStream);
        }
    }
}
