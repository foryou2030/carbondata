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

package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

public class MolapSortTempFileChunkHolder {

    /**
     * LOGGER
     */
    private static final LogService MOLAPCHUNKHOLDERLOGGER =
            LogServiceFactory.getLogService(MolapSortTempFileChunkHolder.class.getName());

    /**
     * temp file
     */
    private File tempFile;

    /**
     * read stream
     */
    private DataInputStream stream;

    /**
     * entry count
     */
    private int entryCount;

    /**
     * number record read
     */
    private int numberOfObjectRead;

    /**
     * return row
     */
    private Object[] returnRow;

    /**
     * number of measures
     */
    private int measureCount;

    /**
     * mdkey length
     */
    private int mdKeyLength;

    /**
     * fileBufferSize for file reader stream size
     */
    private int fileBufferSize;

    private Object[][] currentBuffer;

    private Object[][] backupBuffer;

    private boolean isBackupFilled;

    private ExecutorService executorService;

    private boolean prefetch;

    private int bufferSize;

    private int prefetchRecordsProceesed;

    private int bufferRowCounter;

    private Future<Void> submit;

    /**
     * isFactMdkeyInInputRow
     */
    private boolean isFactMdkeyInInputRow;

    /**
     * factMdkeyLength
     */
    private int factMdkeyLength;

    /**
     * outRecSize
     */
    private int outRecSize;

    /**
     * sortTempFileNoOFRecordsInCompression
     */
    private int sortTempFileNoOFRecordsInCompression;

    /**
     * isSortTempFileCompressionEnabled
     */
    private boolean isSortTempFileCompressionEnabled;

    /**
     * reader
     */
    private AbstractSortTempFileReader reader;

    /**
     * totalRecordFetch
     */
    private int totalRecordFetch;

    private char[] type;

    private String[] aggregator;

    /**
     * highCardCount
     */
    private int highCardCount;

    /**
     * MolapSortTempFileChunkHolder Constructor
     *
     * @param tempFile     temp file
     * @param measureCount measure count
     * @param mdKeyLength  mdkey length
     */
    private MolapSortTempFileChunkHolder(File tempFile, int measureCount, int mdKeyLength,
            int fileBufferSize, boolean isFactMdkeyInInputRow, int factMdkeyLength,
            String[] aggregator, char[] type) {
        // set temp file
        this.tempFile = tempFile;
        // set measure count
        this.measureCount = measureCount;
        // set mdkey length
        this.mdKeyLength = mdKeyLength;
        this.fileBufferSize = fileBufferSize;
        this.executorService = Executors.newFixedThreadPool(1);
        this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
        this.factMdkeyLength = factMdkeyLength;
        this.outRecSize = this.measureCount + 1;
        if (isFactMdkeyInInputRow) {
            this.outRecSize += 1;
        }
        this.aggregator = aggregator;
        this.type = type;
    }

    /**
     * This constructor is used in case of high card dims  needed to be set.
     *
     * @param tmpFile
     * @param measureCount2
     * @param mdkeyLength2
     * @param fileBufferSize2
     * @param isFactMdkeyInInputRow2
     * @param factMdkeyLength2
     * @param aggregators
     * @param highCardCount
     */
    public MolapSortTempFileChunkHolder(File tmpFile, int measureCount2, int mdkeyLength2,
            int fileBufferSize2, boolean isFactMdkeyInInputRow2, int factMdkeyLength2,
            String[] aggregators, int highCardCount, char[] type) {
        this(tmpFile, measureCount2, mdkeyLength2, fileBufferSize2, isFactMdkeyInInputRow2,
                factMdkeyLength2, aggregators, type);
        this.highCardCount = highCardCount;
    }

    /**
     * This method will be used to initialize
     *
     * @throws MolapSortKeyAndGroupByException problem while initializing
     */
    public void initialize() throws MolapSortKeyAndGroupByException {
        prefetch = MolapCommonConstants.MOLAP_PREFETCH_IN_MERGE_VALUE;
        this.isSortTempFileCompressionEnabled = Boolean.parseBoolean(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
                        MolapCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));
        if (this.isSortTempFileCompressionEnabled) {
            MOLAPCHUNKHOLDERLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Compression was used while writing the sortTempFile");
        }
        bufferSize = MolapCommonConstants.MOLAP_PREFETCH_BUFFERSIZE;

        try {
            this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                    MolapProperties.getInstance().getProperty(
                            MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
                            MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
            if (this.sortTempFileNoOFRecordsInCompression < 1) {
                MOLAPCHUNKHOLDERLOGGER
                        .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                "Invalid value of: "
                                        + MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                                        + ": Only Positive Integer value(greater than zero) is allowed.Default value will be used");

                this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                        MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
            }
        } catch (NumberFormatException ex) {
            MOLAPCHUNKHOLDERLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Invalid value of: "
                            + MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                            + ": Only Positive Integer value(greater than zero) is allowed.Default value will be used");
            this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
                    MolapCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
        }

        initialise();
    }

    private void initialise() throws MolapSortKeyAndGroupByException {
        try {
            if (prefetch && isSortTempFileCompressionEnabled) {
                this.bufferSize = sortTempFileNoOFRecordsInCompression;
                reader = new MolapCompressedSortTempFileReader(measureCount, mdKeyLength,
                        isFactMdkeyInInputRow, factMdkeyLength, tempFile, type);
                this.entryCount = reader.getEntryCount();
                new DataFetcher(false).call();
                totalRecordFetch += currentBuffer.length;
                if (totalRecordFetch < this.entryCount) {
                    submit = executorService.submit(new DataFetcher(true));
                }
            } else if (prefetch) {
                reader = new MolapUnComressedSortTempFileReader(measureCount, mdKeyLength,
                        isFactMdkeyInInputRow, factMdkeyLength, tempFile, type);
                this.entryCount = reader.getEntryCount();
                new DataFetcher(false).call();
                totalRecordFetch += currentBuffer.length;
                if (totalRecordFetch < this.entryCount) {
                    submit = executorService.submit(new DataFetcher(true));
                }
            } else if (isSortTempFileCompressionEnabled) {
                this.bufferSize = sortTempFileNoOFRecordsInCompression;
                reader = new MolapCompressedSortTempFileReader(measureCount, mdKeyLength,
                        isFactMdkeyInInputRow, factMdkeyLength, tempFile, type);
                this.entryCount = reader.getEntryCount();
                new DataFetcher(false).call();
            } else {
                stream = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile),
                        this.fileBufferSize));
                this.entryCount = stream.readInt();
            }
        } catch (FileNotFoundException fe) {
            MOLAPCHUNKHOLDERLOGGER
                    .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, fe);
            throw new MolapSortKeyAndGroupByException(tempFile + " No Found", fe);
        } catch (IOException e) {
            MOLAPCHUNKHOLDERLOGGER
                    .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            throw new MolapSortKeyAndGroupByException(tempFile + " No Found", e);
        } catch (Exception e) {
            MOLAPCHUNKHOLDERLOGGER
                    .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            throw new MolapSortKeyAndGroupByException(tempFile + " Problem while reading", e);
        }
    }

    /**
     * This method will be used to read new row from file
     *
     * @throws MolapSortKeyAndGroupByException problem while reading
     */
    public void readRow() throws MolapSortKeyAndGroupByException {
        if (isSortTempFileCompressionEnabled && prefetch) {
            fillDataForPrefetch();
        } else if (prefetch) {
            fillDataForPrefetch();
        } else if (isSortTempFileCompressionEnabled) {
            if (bufferRowCounter >= bufferSize) {
                try {
                    new DataFetcher(false).call();
                    bufferRowCounter = 0;
                } catch (Exception e) {
                    MOLAPCHUNKHOLDERLOGGER
                            .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
                    throw new MolapSortKeyAndGroupByException(tempFile + " Problem while reading",
                            e);
                }

            }
            prefetchRecordsProceesed++;
            returnRow = currentBuffer[bufferRowCounter++];
        } else {
            Object[] outRow = getRowFromStream();
            this.returnRow = outRow;
        }
    }

    /**
     * below method will be used to get the row
     *
     * @return row
     */
    public Object[] getRow() {
        return this.returnRow;
    }

    private void fillDataForPrefetch() {
        if (bufferRowCounter >= bufferSize) {
            if (isBackupFilled) {
                bufferRowCounter = 0;
                currentBuffer = backupBuffer;
                isBackupFilled = false;
                totalRecordFetch += currentBuffer.length;
                if (totalRecordFetch < this.entryCount) {
                    submit = executorService.submit(new DataFetcher(true));
                }
            } else {
                try {
                    submit.get();
                } catch (Exception e) {
                    MOLAPCHUNKHOLDERLOGGER
                            .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
                }
                bufferRowCounter = 0;
                currentBuffer = backupBuffer;
                totalRecordFetch += currentBuffer.length;
                isBackupFilled = false;
                if (totalRecordFetch < this.entryCount) {
                    submit = executorService.submit(new DataFetcher(true));
                }
            }
        }
        prefetchRecordsProceesed++;
        returnRow = currentBuffer[bufferRowCounter++];
    }

    /**
     * @return
     * @throws MolapSortKeyAndGroupByException
     */
    private Object[] getRowFromStream() throws MolapSortKeyAndGroupByException {
        Object[] holder = null;
        byte[] finalByteArr = null;
        // added one for high cardinlaity dims.  
        holder = new Object[this.outRecSize + 1];

        byte[] byteArray = null;
        try {
            for (int i = 0; i < this.aggregator.length - 1; i++) {
                if (type[i] == MolapCommonConstants.BYTE_VALUE_MEASURE
                        || type[i] == MolapCommonConstants.BIG_DECIMAL_MEASURE) {
                    int length = stream.readInt();
                    byteArray = new byte[length];
                    stream.readFully(byteArray);
                    holder[i] = byteArray;
                } else {
                    if (stream.readByte() == MolapCommonConstants.MEASURE_NOT_NULL_VALUE) {
                        if (type[i] == MolapCommonConstants.BIG_INT_MEASURE) {
                            holder[i] = stream.readLong();
                        } else {
                        holder[i] = stream.readDouble();
                    }
                }
            }
            }
            holder[this.aggregator.length - 1] = stream.readDouble();

            //Read byte [] of high cardinality from stream.
            if (highCardCount > 0) {
                short lengthOfByteArray = stream.readShort();
                ByteBuffer buff = ByteBuffer.allocate(lengthOfByteArray + 2);
                buff.putShort(lengthOfByteArray);
                byte[] byteArr = new byte[lengthOfByteArray];
                stream.readFully(byteArr);

                buff.put(byteArr);
                finalByteArr = buff.array();

            }
            holder[measureCount] = finalByteArr;

            byteArray = new byte[mdKeyLength];
            // read mdkey
            if (stream.read(byteArray) < 0) {
                MOLAPCHUNKHOLDERLOGGER
                        .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                "Problme while reading the mdkey fom sort temp file");
            }
            holder[measureCount + 1] = byteArray;
            if (isFactMdkeyInInputRow) {
                byteArray = new byte[this.factMdkeyLength];
                if (stream.read(byteArray) < 0) {
                    MOLAPCHUNKHOLDERLOGGER
                            .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                    "Problme while reading the fact mdkey fom sort temp file");
                }
                holder[holder.length - 1] = byteArray;
            }
            // set mdkey
            // increment number if record read
            this.numberOfObjectRead++;
            // return out row
        } catch (IOException ex) {
            MOLAPCHUNKHOLDERLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Problme while reading the madkey fom sort temp file", ex);
            throw new MolapSortKeyAndGroupByException("Problem while reading the sort temp file ",
                    ex);
        }
        return holder;
    }

    /**
     * Below method will be used to close streams
     */
    public void closeStream() {
        MolapUtil.closeStreams(stream);
        if (null != reader) {
            reader.finish();
        }
        executorService.shutdown();
    }

    /**
     * below method will be used to check whether any more records are present
     * in file or not
     *
     * @return more row present in file
     */
    public boolean hasNext() {
        if (prefetch || isSortTempFileCompressionEnabled) {
            return this.prefetchRecordsProceesed < this.entryCount;
        }
        return this.numberOfObjectRead < this.entryCount;
    }

    /**
     * This method will number of entries
     *
     * @return entryCount
     */
    public int getEntryCount() {
        return entryCount;
    }

    /**
     * @return the tempFile
     */
    public File getTempFile() {
        return tempFile;
    }

    /**
     * @param tempFile the tempFile to set
     */
    public void setTempFile(File tempFile) {
        this.tempFile = tempFile;
    }

    private final class DataFetcher implements Callable<Void> {
        private boolean isBackUpFilling;

        private DataFetcher(boolean backUp) {
            isBackUpFilling = backUp;
        }

        @Override public Void call() throws Exception {
            try {
                if (isBackUpFilling) {
                    isBackupFilled = true;
                    backupBuffer = reader.getRow();
                } else {
                    currentBuffer = reader.getRow();
                }
            } catch (Exception e) {
                MOLAPCHUNKHOLDERLOGGER
                        .error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            }
            return null;
        }

    }

}
