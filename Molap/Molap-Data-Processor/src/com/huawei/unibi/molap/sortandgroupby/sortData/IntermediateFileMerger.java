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
import java.math.BigDecimal;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.*;

public class IntermediateFileMerger implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(IntermediateFileMerger.class.getName());

    /**
     * recordHolderHeap
     */
    private AbstractQueue<SortTempFileChunkHolder> recordHolderHeap;

    /**
     * fileCounter
     */
    private int fileCounter;

    /**
     * stream
     */
    private DataOutputStream stream;

    /**
     * totalNumberOfRecords
     */
    private int totalNumberOfRecords;

    /**
     * records
     */
    private Object[][] records;

    /**
     * entryCount
     */
    private int entryCount;

    /**
     * writer
     */
    private TempSortFileWriter writer;

    /**
     * totalSize
     */
    private int totalSize;

    private FileMergerParameters mergerParameters;

    /**
     * IntermediateFileMerger Constructor
     */
    public IntermediateFileMerger(FileMergerParameters mergerParameters) {
        this.mergerParameters = mergerParameters;
        this.fileCounter = mergerParameters.getIntermediateFiles().length;
    }

    @Override public Void call() throws Exception {
        boolean isFailed = false;
        try {
            startSorting();
            initialize();

            while (hasNext()) {
                writeDataTofile(next());
            }
            if (mergerParameters.isCompressionEnabled() || mergerParameters.isPrefetch()) {
                if (entryCount > 0) {
                    if (entryCount < totalSize) {
                        Object[][] temp = new Object[entryCount][];
                        System.arraycopy(records, 0, temp, 0, entryCount);
                        records = temp;
                        this.writer.writeSortTempFile(temp);
                    } else {
                        this.writer.writeSortTempFile(records);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Problem while intermediate merging");
            isFailed = true;
        } finally {
            records = null;
            MolapUtil.closeStreams(this.stream);
            if (null != writer) {
                writer.finish();
            }
            if (!isFailed) {
                try {
                    finish();
                } catch (MolapSortKeyAndGroupByException e) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                            "Problem while deleting the merge file");
                }
            } else {
                if (mergerParameters.getOutFile().delete()) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while deleting the merge file");
                }
            }
        }

        return null;
    }

    /**
     * This method is responsible for initializing the out stream
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void initialize() throws MolapSortKeyAndGroupByException {
        if (!mergerParameters.isCompressionEnabled() && !mergerParameters.isPrefetch()) {
            try {
                this.stream = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(mergerParameters.getOutFile()),
                        mergerParameters.getFileWriteBufferSize()));
                this.stream.writeInt(this.totalNumberOfRecords);
            } catch (FileNotFoundException e) {
                throw new MolapSortKeyAndGroupByException("Problem while getting the file", e);
            } catch (IOException e) {
                throw new MolapSortKeyAndGroupByException("Problem while writing the data to file",
                        e);
            }
        } else {
            writer = TempSortFileWriterFactory.getInstance()
                    .getTempSortFileWriter(mergerParameters.isCompressionEnabled(),
                            mergerParameters.getDimColCount(),
                            mergerParameters.getComplexDimColCount(),
                            mergerParameters.getMeasureColCount(),
                            mergerParameters.getHighCardinalityCount(),
                            mergerParameters.getFileWriteBufferSize());
            writer.initiaize(mergerParameters.getOutFile(), totalNumberOfRecords);

            if (mergerParameters.isPrefetch()) {
                totalSize = mergerParameters.getPrefetchBufferSize();
            } else {
                totalSize = mergerParameters.getNoOfRecordsInCompression();
            }
        }
    }

    /**
     * This method will be used to get the sorted record from file
     *
     * @return sorted record sorted record
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[] getSortedRecordFromFile() throws MolapSortKeyAndGroupByException {
        Object[] row = null;

        // poll the top object from heap
        // heap maintains binary tree which is based on heap condition that will
        // be based on comparator we are passing the heap
        // when will call poll it will always delete root of the tree and then
        // it does trickel down operation complexity is log(n)
        SortTempFileChunkHolder poll = this.recordHolderHeap.poll();

        // get the row from chunk
        row = poll.getRow();

        // check if there no entry present
        if (!poll.hasNext()) {
            // if chunk is empty then close the stream
            poll.closeStream();

            // change the file counter
            --this.fileCounter;

            // reaturn row
            return row;
        }

        // read new row
        poll.readRow();

        // add to heap
        this.recordHolderHeap.add(poll);

        // return row
        return row;
    }

    /**
     * Below method will be used to start storing process This method will get
     * all the temp files present in sort temp folder then it will create the
     * record holder heap and then it will read first record from each file and
     * initialize the heap
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void startSorting() throws MolapSortKeyAndGroupByException {
        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Number of temp file: " + this.fileCounter);

        // create record holder heap
        createRecordHolderQueue(mergerParameters.getIntermediateFiles());

        // iterate over file list and create chunk holder and add to heap
        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Started adding first record from each file");

        SortTempFileChunkHolder sortTempFileChunkHolder = null;

        for (File tempFile : mergerParameters.getIntermediateFiles()) {
            // create chunk holder
            sortTempFileChunkHolder =
                    new SortTempFileChunkHolder(tempFile, mergerParameters.getDimColCount(),
                            mergerParameters.getComplexDimColCount(),
                            mergerParameters.getMeasureColCount(),
                            mergerParameters.getFileReadBufferSize(),
                            mergerParameters.getHighCardinalityCount(),
                            mergerParameters.getAggType());

            // initialize
            sortTempFileChunkHolder.initialize();
            sortTempFileChunkHolder.readRow();
            this.totalNumberOfRecords += sortTempFileChunkHolder.getEntryCount();

            // add to heap
            this.recordHolderHeap.add(sortTempFileChunkHolder);
        }

        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Heap Size" + this.recordHolderHeap.size());
    }

    /**
     * This method will be used to create the heap which will be used to hold
     * the chunk of data
     *
     * @param listFiles list of temp files
     */
    private void createRecordHolderQueue(File[] listFiles) {
        // creating record holder heap
        this.recordHolderHeap = new PriorityQueue<SortTempFileChunkHolder>(listFiles.length,
                new Comparator<SortTempFileChunkHolder>() {
                    public int compare(SortTempFileChunkHolder holderA,
                            SortTempFileChunkHolder holderB) {
                        Object[] rowA = holderA.getRow();
                        Object[] rowB = holderB.getRow();
                        int diff = 0;

                        for (int i = 0; i < mergerParameters.getDimColCount(); i++) {
                            int dimFieldA = (Integer) RemoveDictionaryUtil.getDimension(i, rowA);
                            int dimFieldB = (Integer) RemoveDictionaryUtil.getDimension(i, rowB);

                            diff = dimFieldA - dimFieldB;
                            if (diff != 0) {
                                return diff;
                            }
                        }
                        return diff;
                    }
                });
    }

    /**
     * This method will be used to get the sorted row
     *
     * @return sorted row
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[] next() throws MolapSortKeyAndGroupByException {
        return getSortedRecordFromFile();
    }

    /**
     * This method will be used to check whether any more element is present or
     * not
     *
     * @return more element is present
     */
    private boolean hasNext() {
        return this.fileCounter > 0;
    }

    /**
     * Below method will be used to write data to file
     *
     * @throws MolapSortKeyAndGroupByException problem while writing
     */
    private void writeDataTofile(Object[] row) throws MolapSortKeyAndGroupByException {
        if (mergerParameters.isCompressionEnabled() || mergerParameters.isPrefetch()) {
            if (entryCount == 0) {
                records = new Object[totalSize][];
                records[entryCount++] = row;
                return;
            }

            records[entryCount++] = row;
            if (entryCount == totalSize) {
                this.writer.writeSortTempFile(records);
                entryCount = 0;
                records = new Object[totalSize][];
            }
            return;
        }
        try {
            int fieldIndex = 0;
            char[] aggType = mergerParameters.getAggType();

            for (int counter = 0; counter < mergerParameters.getDimColCount(); counter++) {
                stream.writeInt((Integer) RemoveDictionaryUtil.getDimension(fieldIndex++, row));
            }

            // added for high card also
            if (mergerParameters.getHighCardinalityCount() > 0) {
                stream.write(RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row));
            }

            fieldIndex = 0;
            for (int counter = 0; counter < mergerParameters.getMeasureColCount(); counter++) {
                if (null != RemoveDictionaryUtil.getMeasure(fieldIndex, row)) {
                    stream.write((byte) 1);
                    if (aggType[counter] == MolapCommonConstants.BYTE_VALUE_MEASURE) {
                        Double val = (Double) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
                        stream.writeDouble(val);
                    } else if (aggType[counter] == MolapCommonConstants.BIG_INT_MEASURE) {
                        Long val = (Long) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
                        stream.writeLong(val);
                    } else if (aggType[counter] == MolapCommonConstants.BIG_DECIMAL_MEASURE) {
                        BigDecimal val =
                                (BigDecimal) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
                        byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
                        stream.writeInt(bigDecimalInBytes.length);
                        stream.write(bigDecimalInBytes);
                    }
                } else {
                    stream.write((byte) 0);
                }

                fieldIndex++;
            }

        } catch (IOException e) {
            throw new MolapSortKeyAndGroupByException("Problem while writing the file", e);
        }
    }

    private void finish() throws MolapSortKeyAndGroupByException {
        if (recordHolderHeap != null) {
            int size = recordHolderHeap.size();
            for (int i = 0; i < size; i++) {
                recordHolderHeap.poll().closeStream();
            }
        }
        try {
            MolapUtil.deleteFiles(mergerParameters.getIntermediateFiles());
        } catch (MolapUtilException e) {
            throw new MolapSortKeyAndGroupByException(
                    "Problem while deleting the intermediate files");
        }
    }
}
