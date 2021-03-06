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

package com.huawei.unibi.molap.store;

import java.io.File;
import java.io.FileFilter;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.sortandgroupby.sortKey.MolapSortTempFileChunkHolder;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;
import com.huawei.unibi.molap.util.MolapProperties;

public class SingleThreadFinalMerger {

    /**
     * LOGGER
     */
    private static final LogService SINGLETHREADLOGGER =
            LogServiceFactory.getLogService(SingleThreadFinalMerger.class.getName());

    /**
     * CONSTANT_SIZE_TEN
     */
    private static final int CONSTANT_SIZE_TEN = 10;

    /**
     * lockObject
     */
    private static final Object LOCKOBJECT = new Object();

    /**
     * fileCounter
     */
    private int fileCounter;

    /**
     * fileBufferSize
     */
    private int fileBufferSize;

    /**
     * recordHolderHeap
     */
    private AbstractQueue<MolapSortTempFileChunkHolder> recordHolderHeapLocal;

    /**
     * tableName
     */
    private String tableName;

    /**
     * measureCount
     */
    private int measureCount;

    /**
     * mdKeyIndex
     */
    private int mdKeyIndex;

    /**
     * mdkeyLength
     */
    private int mdkeyLength;

    /**
     * tempFileLocation
     */
    private String tempFileLocation;

    /**
     * isFactMdkeyInInputRow
     */
    private boolean isFactMdkeyInInputRow;

    /**
     * factMdkeyLength
     */
    private int factMdkeyLength;

    private char[] type;

    private String[] aggregators;
    /**
     * highCardCount
     */
    private int highCardCount;

    public SingleThreadFinalMerger(String tempFileLocation, String tableName, int mdkeyLength,
            int measureCount, int mdKeyIndex, boolean isFactMdkeyInInputRow, int factMdkeyLength,
            char[] type, String[] aggregators, int highCardCount) {
        this.tempFileLocation = tempFileLocation;
        this.tableName = tableName;
        this.measureCount = measureCount;
        this.mdKeyIndex = mdKeyIndex;
        this.mdkeyLength = mdkeyLength;
        this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
        this.factMdkeyLength = factMdkeyLength;
        this.type = type;
        this.aggregators = aggregators;
        this.highCardCount = highCardCount;
    }

    /**
     * This method will be used to merger the merged files
     *
     * @throws MolapSortKeyAndGroupByException
     */
    public void startFinalMerge() throws MolapDataWriterException {
        // get all the merged files 
        File file = new File(tempFileLocation);
        File[] fileList = file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(tableName) && !pathname.getName()
                        .endsWith(MolapCommonConstants.CHECKPOINT_EXT);
            }
        });
        if (null == fileList || fileList.length < 0) {
            return;
        }
        startSorting(fileList);
    }

    /**
     * Below method will be used to start storing process This method will get
     * all the temp files present in sort temp folder then it will create the
     * record holder heap and then it will read first record from each file and
     * initialize the heap
     *
     * @throws MolapSortKeyAndGroupByException
     */
    private void startSorting(File[] files) throws MolapDataWriterException {
        this.fileCounter = files.length;
        this.fileBufferSize = MolapDataProcessorUtil
                .getFileBufferSize(this.fileCounter, MolapProperties.getInstance(),
                        CONSTANT_SIZE_TEN);
        SINGLETHREADLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Number of temp file: " + this.fileCounter);

        SINGLETHREADLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "File Buffer Size: " + this.fileBufferSize);
        // create record holder heap
        createRecordHolderQueue(files);
        // iterate over file list and create chunk holder and add to heap
        SINGLETHREADLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Started adding first record from each file");
        int maxThreadForSorting = 0;
        try {
            maxThreadForSorting = Integer.parseInt(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.MOLAP_MAX_THREAD_FOR_SORTING,
                            MolapCommonConstants.MOLAP_MAX_THREAD_FOR_SORTING_DEFAULTVALUE));
        } catch (NumberFormatException ex) {
            maxThreadForSorting = Integer.parseInt(
                    MolapCommonConstants.MOLAP_MAX_THREAD_FOR_SORTING_DEFAULTVALUE);
        }
        ExecutorService service = Executors.newFixedThreadPool(maxThreadForSorting);
        for (final File tmpFile : files) {

            Callable<Void> runnable = new Callable<Void>() {
                @Override public Void call() throws MolapSortKeyAndGroupByException {
                    // create chunk holder
                    MolapSortTempFileChunkHolder molapSortTempFileChunkHolder =
                            new MolapSortTempFileChunkHolder(tmpFile, measureCount, mdkeyLength,
                                    fileBufferSize, isFactMdkeyInInputRow, factMdkeyLength,
                                    aggregators, highCardCount, type);
                    // initialize
                    molapSortTempFileChunkHolder.initialize();
                    molapSortTempFileChunkHolder.readRow();
                    synchronized (LOCKOBJECT) {
                        recordHolderHeapLocal.add(molapSortTempFileChunkHolder);
                    }
                    // add to heap
                    return null;

                }
            };
            service.submit(runnable);
        }
        service.shutdown();
        try {
            service.awaitTermination(2, TimeUnit.HOURS);
        } catch (Exception ex) {
            throw new MolapDataWriterException(ex.getMessage(), ex);
        }
        SINGLETHREADLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Heap Size" + this.recordHolderHeapLocal.size());
    }

    /**
     * This method will be used to create the heap which will be used to hold
     * the chunk of data
     *
     * @param listFiles list of temp files
     */
    private void createRecordHolderQueue(File[] listFiles) {
        // creating record holder heap
        this.recordHolderHeapLocal =
                new PriorityQueue<MolapSortTempFileChunkHolder>(listFiles.length,
                        new Comparator<MolapSortTempFileChunkHolder>() {
                            public int compare(MolapSortTempFileChunkHolder r1,
                                    MolapSortTempFileChunkHolder r2) {
                                byte[] b1 = (byte[]) r1.getRow()[mdKeyIndex];
                                byte[] b2 = (byte[]) r2.getRow()[mdKeyIndex];
                                int cmp = 0;
                                int a = 0;
                                int b = 0;
                                for (int i = 0; i < b1.length; i++) {
                                    a = b1[i] & 0xFF;
                                    b = b2[i] & 0xFF;
                                    cmp = a - b;
                                    if (cmp != 0) {
                                        return cmp;
                                    }
                                }
                                return cmp;
                            }
                        });
    }

    /**
     * This method will be used to get the sorted row
     *
     * @return sorted row
     * @throws MolapSortKeyAndGroupByException
     */
    public Object[] next() throws MolapDataWriterException {
        return getSortedRecordFromFile();
    }

    /**
     * This method will be used to check whether any more element is present or
     * not
     *
     * @return more element is present
     */
    public boolean hasNext() {
        return this.fileCounter > 0;
    }

    /**
     * This method will be used to get the sorted record from file
     *
     * @return sorted record sorted record
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[] getSortedRecordFromFile() throws MolapDataWriterException {
        Object[] row = null;
        // poll the top object from heap
        // heap maintains binary tree which is based on heap condition that will
        // be based on comparator we are passing the heap
        // when will call poll it will always delete root of the tree and then
        // it does trickel down operation complexity is log(n)
        MolapSortTempFileChunkHolder chunkPoll = this.recordHolderHeapLocal.poll();
        // get the row from chunk
        row = chunkPoll.getRow();
        // check if there no entry present
        if (!chunkPoll.hasNext()) {
            // if chunk is empty then close the stream
            chunkPoll.closeStream();
            // change the file counter
            --this.fileCounter;
            // reaturn row
            return row;
        }
        // read new row
        try {
            chunkPoll.readRow();
        } catch (MolapSortKeyAndGroupByException e) {
            throw new MolapDataWriterException(e.getMessage(), e);
        }
        // add to heap
        this.recordHolderHeapLocal.add(chunkPoll);
        // return row
        return row;
    }

    public void clear() {
        if (null != recordHolderHeapLocal) {
            recordHolderHeapLocal = null;
        }
    }
}
