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

import java.io.*;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapUtil;

public abstract class AbstractTempSortFileWriter implements TempSortFileWriter {

    /**
     * writeFileBufferSize
     */
    protected int writeBufferSize;

    /**
     * Measure count
     */
    protected int measureCount;

    /**
     * Measure count
     */
    protected int dimensionCount;

    /**
     * complexDimension count
     */
    protected int complexDimensionCount;

    /**
     * stream
     */
    protected DataOutputStream stream;

    /**
     * highCardinalityCount
     */
    protected int highCardinalityCount;

    /**
     * AbstractTempSortFileWriter
     *
     * @param writeBufferSize
     * @param dimensionCount
     * @param measureCount
     */
    public AbstractTempSortFileWriter(int dimensionCount, int complexDimensionCount,
            int measureCount, int highCardinalityCount, int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        this.dimensionCount = dimensionCount;
        this.complexDimensionCount = complexDimensionCount;
        this.measureCount = measureCount;
        this.highCardinalityCount = highCardinalityCount;
    }

    /**
     * Below method will be used to initialize the stream and write the entry count
     */
    @Override public void initiaize(File file, int entryCount)
            throws MolapSortKeyAndGroupByException {
        try {
            stream = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(file), writeBufferSize));
            stream.writeInt(entryCount);
        } catch (FileNotFoundException e1) {
            throw new MolapSortKeyAndGroupByException(e1);
        } catch (IOException e) {
            throw new MolapSortKeyAndGroupByException(e);
        }
    }

    /**
     * Below method will be used to close the stream
     */
    @Override public void finish() {
        MolapUtil.closeStreams(stream);
    }
}
