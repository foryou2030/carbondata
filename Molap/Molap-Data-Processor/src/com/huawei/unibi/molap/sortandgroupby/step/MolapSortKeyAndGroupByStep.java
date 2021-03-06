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

package com.huawei.unibi.molap.sortandgroupby.step;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvreader.checkpoint.CheckPointHanlder;
import com.huawei.unibi.molap.csvreader.checkpoint.CheckPointInterface;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.exception.MolapDataProcessorException;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.schema.metadata.SortObserver;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.sortandgroupby.sortKey.MolapSortKeys;
import com.huawei.unibi.molap.util.*;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

//import org.pentaho.di.core.exception.KettleStepException;
//import org.pentaho.di.core.exception.KettleStepException;
//import com.huawei.unibi.molap.datastorage.store.compression.MeasureMetaDataModel;
//import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;

public class MolapSortKeyAndGroupByStep extends BaseStep {

    /**
     * LOGGER
     */
    private static final LogService SORTKEYSTEPLOGGER =
            LogServiceFactory.getLogService(MolapSortKeyAndGroupByStep.class.getName());
    /**
     * decimalPointers
     */
    private final byte decimalPointers = Byte.parseByte(MolapProperties.getInstance()
            .getProperty(MolapCommonConstants.MOLAP_DECIMAL_POINTERS,
                    MolapCommonConstants.MOLAP_DECIMAL_POINTERS_DEFAULT));
    /**
     * MolapSortKeyAndGroupByStepData
     */
    private MolapSortKeyAndGroupByStepData data;
    /**
     * MolapSortKeyAndGroupByStepMeta
     */
    private MolapSortKeyAndGroupByStepMeta meta;
    /**
     * molapSortKeys
     */
    private MolapSortKeys molapSortKeys;
    /**
     * rowCounter
     */
    private long readCounter;
    /**
     * writeCounter
     */
    private long writeCounter;
    /**
     * logCounter
     */
    private int logCounter;
    /**
     * mdkeyIndex
     */
    private int mdkeyIndex;
    /**
     * mdkeylength
     */
    private int mdkeylength;
    /**
     * checkpoint
     */
    private CheckPointInterface checkpoint;
    /**
     * observer
     */
    private SortObserver observer;
    /**
     * minValue
     */
    private Object[] minValue;

    /**
     * minValue
     */
    private Object[] maxValue;

    /**
     * minValue
     */
    private int[] decimalLength;
    /**
     * minValue
     */
    private Object[] uniqueValue;

    /**
     * minValue
     */
    private char[] aggType;
    private String[] aggregators;
    /**
     * store location
     */
    private String createStoreLocaion;

    /**
     * MolapSortKeyAndGroupByStep Constructor
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public MolapSortKeyAndGroupByStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
            int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Perform the equivalent of processing one row. Typically this means
     * reading a row from input (getRow()) and passing a row to output
     * (putRow)).
     *
     * @param smi The steps metadata to work with
     * @param sdi The steps temporary working data to work with (database
     *            connections, result sets, caches, temporary variables, etc.)
     * @return false if no more rows can be processed or an error occurred.
     * @throws KettleException
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        // get step meta 
        this.meta = ((MolapSortKeyAndGroupByStepMeta) smi);
        // get step data 
        this.data = ((MolapSortKeyAndGroupByStepData) sdi);
        // get row 
        Object[] row = getRow();

        // check point 
        if (!meta.isAutoAggRequest() || !meta.isUpdateMemberRequest()) {
            checkpoint =
                    CheckPointHanlder.getCheckpoint(new File(getTrans().getFilename()).getName());
        } else {
            checkpoint = CheckPointHanlder.getDummyCheckPoint();
        }

        // create sort observer
        this.observer = new SortObserver();

        // if row is null then this step can start processing the data 
        if (row == null) {
            return processRowToNextStep();
        }
        // check if all records are null than send empty row to next step
        else if (MolapDataProcessorUtil.checkAllValuesAreNull(row)) {
            // create empty row out size 
            int outSize = Integer.parseInt(meta.getOutputRowSize());

            if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !(meta.isAutoAggRequest() || meta
                    .isUpdateMemberRequest())) {
                outSize = outSize - 2;
            }
            Object[] outRow = new Object[outSize];
            // clone out row meta 
            this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());
            // get all fields 
            this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);

            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTabelName());
            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Record Form Previous Step was null");
            String logMessage = "Summary: Molap Sort Key Step: Read: " + 1 + ": Write: " + 1;
            SORTKEYSTEPLOGGER
                    .info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
            putRow(data.getOutputRowMeta(), outRow);
            setOutputDone();
            return false;
        }
        // if first 
        if (first) {
            first = false;
            // clone out row meta
            this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());
            // get all fields 
            this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);
            this.meta.initialize();
            // get mdkey index

            // create sort key
            int factMDkeySize = 0;

            if (meta.isFactMdKeyInInputRow() && meta.isAutoAggRequest()) {
                factMDkeySize = ((byte[]) row[row.length - 1]).length;
            }
            initAggregators(row);
            aggType = getAggtype();
            initializeMeasureIndex(row);
            initialize();
            this.mdkeyIndex = row.length - 1;
            this.mdkeylength = meta.getMdkeyLength();
            this.molapSortKeys =
                    new MolapSortKeys(meta.getTabelName(), aggregators.length, mdkeyIndex,
                            mdkeylength, this.checkpoint, this.observer, meta.isAutoAggRequest(),
                            meta.isFactMdKeyInInputRow(), factMDkeySize, this.aggregators,
                            meta.getAggregatorClass(),
                            MolapDataProcessorUtil.getDimLens(meta.getFactDimLensString()),
                            meta.getSchemaName(), meta.getCubeName(), meta.isUpdateMemberRequest(),
                            meta.getHighCardinalityCount(), aggType);
            try {
                // initialize sort
                this.molapSortKeys.initialize(meta.getSchemaName(), meta.getCubeName(),
                        meta.getCurrentRestructNumber());
            } catch (MolapSortKeyAndGroupByException e) {
                throw new KettleException(e);
            }
            this.logCounter =
                    Integer.parseInt(MolapCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER);
        }
        readCounter++;
        if (readCounter % logCounter == 0) {
            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTabelName());
            String logMessage = "Molap Sort Key Step: Record Read: " + readCounter;
            SORTKEYSTEPLOGGER
                    .info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
        }
        try {
            //check for minimum value
            calculateMaxMinUnique(row);
            // add row 
            this.molapSortKeys.addRow(row);
        } catch (Throwable e) {
            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            throw new KettleException(e);
        }

        return true;
    }

    private char[] getAggtype() {
        String[] aggMeasures = meta.getAggregateMeasuresColumnNameString()
                .split(MolapCommonConstants.HASH_SPC_CHARACTER);
        char[] aggType = new char[aggMeasures.length];
        Arrays.fill(aggType, MolapCommonConstants.SUM_COUNT_VALUE_MEASURE);
        Cube cube = MolapMetadata.getInstance()
                .getCube(meta.getSchemaName() + '_' + meta.getCubeName());
        for (int i = 0; i < aggMeasures.length - 1; i++) {
            Measure measure = cube.getMeasure(cube.getFactTableName(), aggMeasures[i]);
            if (null == measure) {
                aggType[i] = MolapUtil.getType(this.aggregators[i]);
            } else {
                aggType[i] = DataTypeUtil.getAggType(measure.getDataType(), this.aggregators[i]);
            }
        }
        return aggType;
    }

    /**
     * Below method will be used to process data to next step
     *
     * @return false is finished
     * @throws KettleException
     */
    private boolean processRowToNextStep() throws KettleException {
        // in case of check point when last time graph executed it finished
        // all the temp file writing so in that case from csv step we will first
        // row as null but as sort temp files are present we can start stroing
        // form there
        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && null == this.molapSortKeys && !(
                meta.isAutoAggRequest() || meta.isUpdateMemberRequest())) {
            // check if sorting resume is required 
            if (MolapSortKeys.isSortingResumeRequired(meta.getSchemaName(), meta.getCubeName(),
                    meta.getTabelName(), meta.getCurrentRestructNumber())) {
                putRow(data.getOutputRowMeta(), new Object[0]);
                setOutputDone();
                return false;
            }
        }
        if (null == this.molapSortKeys) {

            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTabelName());
            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Number of Records was Zero");
            String logMessage = "Summary: Molap Sort Key Step: Read: " + 0 + ": Write: " + 0;
            SORTKEYSTEPLOGGER
                    .info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
            putRow(data.getOutputRowMeta(), new Object[0]);
            setOutputDone();
            return false;
        }
        try {
            // start sorting
            this.molapSortKeys.startSorting();
            writeMeasureMetadataFile();
            // check any more rows are present 
            SORTKEYSTEPLOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTabelName());
            String logMessage = "Summary: Molap Sort Key Step: Read: " + readCounter + ": Write: "
                    + writeCounter;
            SORTKEYSTEPLOGGER
                    .info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
            putRow(data.getOutputRowMeta(), new Object[0]);
            setOutputDone();
            return false;
        } catch (MolapSortKeyAndGroupByException me) {
            throw new KettleException(me);
        }

    }

    /**
     *
     *
     */
    private void initializeMeasureIndex(Object[] row) {
        MeasureAggregator[] aggregator = (MeasureAggregator[]) row[0];
        minValue = new Object[aggregator.length + 1];
        maxValue = new Object[aggregator.length + 1];
        uniqueValue = new Object[aggregator.length + 1];
        decimalLength = new int[aggregator.length + 1];
        for (int i = 0; i < aggregator.length; i++) {
            if (aggType[i] == MolapCommonConstants.BIG_INT_MEASURE) {
                maxValue[i] = Long.MIN_VALUE;
            } else if (aggType[i] == MolapCommonConstants.SUM_COUNT_VALUE_MEASURE) {
            maxValue[i] = -Double.MAX_VALUE;
            } else if (aggType[i] == MolapCommonConstants.BIG_DECIMAL_MEASURE) {
                maxValue[i] = new BigDecimal(0.0);
            } else {
                maxValue[i] = 0.0;
            }
            if (aggType[i] == MolapCommonConstants.BIG_INT_MEASURE) {
                minValue[i] = Long.MAX_VALUE;
            } else if (aggType[i] == MolapCommonConstants.SUM_COUNT_VALUE_MEASURE) {
            minValue[i] = Double.MAX_VALUE;
            } else if (aggType[i] == MolapCommonConstants.BIG_DECIMAL_MEASURE) {
                minValue[i] = new BigDecimal(Double.MAX_VALUE);
            } else {
                minValue[i] = 0.0;
            }
            decimalLength[i] = 0;
        }
        minValue[minValue.length - 1] = 1.0;
        maxValue[maxValue.length - 1] = 1.0;
        uniqueValue[minValue.length - 1] = 0.0;
        decimalLength[decimalLength.length - 1] = 0;
        calculateMaxMinUnique(row);
    }

    private void initAggregators(Object[] row) {
        MeasureAggregator[] aggregator = (MeasureAggregator[]) row[0];
        this.aggregators = new String[aggregator.length + 1];
        for (int i = 0; i < aggregator.length; i++) {
            this.aggregators[i] = MolapDataProcessorUtil.getAggType(aggregator[i]);
        }
        this.aggregators[aggregators.length - 1] = MolapCommonConstants.COUNT;
    }

    /**
     * This method will be used to update the max value for each measure
     */
    private void calculateMaxMinUnique(Object[] row) {
        MeasureAggregator[] aggregator = (MeasureAggregator[]) row[0];
        for (int i = 0; i < aggregator.length; i++) {
            if (aggType[i] == MolapCommonConstants.SUM_COUNT_VALUE_MEASURE) {
                double prevMaxVal = (double) maxValue[i];
                double prevMinVal = (double) minValue[i];
                double value = aggregator[i].getDoubleValue();
                maxValue[i] = (prevMaxVal > value ? maxValue[i] : value);
                minValue[i] = (prevMinVal < value ? minValue[i] : value);
                uniqueValue[i] = (double) minValue[i] - 1;
                int num = (value % 1 == 0) ? 0 : decimalPointers;
                decimalLength[i] = (decimalLength[i] > num ? decimalLength[i] : num);
            } else if (aggType[i] == MolapCommonConstants.BIG_INT_MEASURE) {
                long prevMaxVal = (long) maxValue[i];
                long prevMinVal = (long) minValue[i];
                long value = aggregator[i].getLongValue();
                maxValue[i] = (prevMaxVal > value ? maxValue[i] : value);
                minValue[i] = (prevMinVal < value ? minValue[i] : value);
                uniqueValue[i] = (long) minValue[i] - 1;
            int num = (value % 1 == 0) ? 0 : decimalPointers;
            decimalLength[i] = (decimalLength[i] > num ? decimalLength[i] : num);
            } else if (aggType[i] == MolapCommonConstants.BIG_DECIMAL_MEASURE) {
                BigDecimal val = (BigDecimal) minValue[i];
                BigDecimal newVal = aggregator[i].getBigDecimalValue();
                val = val.min(newVal);
                minValue[i] = val;
                uniqueValue[i] = (val.subtract(new BigDecimal(1.0)));
            } else {
                uniqueValue[i] = 0.0;
            }
        }
        double value = (Double) row[1];
        row[1] = 1.0d;
        minValue[minValue.length - 1] = ((double) minValue[minValue.length - 1] < value ?
                minValue[minValue.length - 1] :
                value);
        maxValue[maxValue.length - 1] = ((double) maxValue[maxValue.length - 1] > value ?
                maxValue[maxValue.length - 1] :
                value);
    }

    /**
     * @throws KettleException
     */
    private void initialize() throws KettleException {
        String[] aggreateLevels = meta.getAggregateLevels();
        String[] factLevels = meta.getFactLevels();
        int[] cardinality = meta.getFactDimLens();
        int[] aggCardinality = new int[aggreateLevels.length - meta.getHighCardinalityCount()];
        Arrays.fill(aggCardinality, -1);
        for (int k = 0; k < aggreateLevels.length; k++) {
            for (int j = 0; j < factLevels.length; j++) {
                if (aggreateLevels[k].equals(factLevels[j])) {
                    aggCardinality[k] = cardinality[j];
                    break;
                }
            }
        }
        meta.setAggDimeLens(aggCardinality);
        createStoreAndWriteSliceMetadata(meta.isManualAutoAggRequest(), aggCardinality);
    }

    /**
     * Below method will be used to create the load folder and write the slice
     * meta data for aggregate table
     *
     * @throws KettleException
     */
    private void createStoreAndWriteSliceMetadata(boolean deleteExistingStore, int[] aggCardinality)
            throws KettleException {
        createStoreLocaion = MolapDataProcessorUtil
                .createStoreLocaion(meta.getSchemaName(), meta.getCubeName(), meta.getTabelName(),
                        deleteExistingStore, meta.getCurrentRestructNumber());
        updateAndWriteSliceMetadataFile(createStoreLocaion);
        writeAggLevelCardinalityFile(aggCardinality, createStoreLocaion);
    }

    /**
     * This method writes aggregate level cardinality of each agg level to a
     * file
     *
     * @param dimCardinality
     * @param storeLocation
     * @throws KettleException
     */
    private void writeAggLevelCardinalityFile(int[] dimCardinality, String storeLocation)
            throws KettleException {
        String aggLevelCardinalityFilePath =
                storeLocation + File.separator + MolapCommonConstants.LEVEL_METADATA_FILE + meta
                        .getTabelName() + ".metadata";

        FileOutputStream fileOutputStream = null;
        FileChannel channel = null;
        try {
            int dimCardinalityArrLength = dimCardinality.length;

            // first four bytes for writing the length of array, remaining for
            // array data
            ByteBuffer buffer = ByteBuffer.allocate(MolapCommonConstants.INT_SIZE_IN_BYTE
                    + dimCardinalityArrLength * MolapCommonConstants.INT_SIZE_IN_BYTE);

            fileOutputStream = new FileOutputStream(aggLevelCardinalityFilePath);
            channel = fileOutputStream.getChannel();
            buffer.putInt(dimCardinalityArrLength);

            for (int i = 0; i < dimCardinalityArrLength; i++) {
                buffer.putInt(dimCardinality[i]);
            }

            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        } catch (IOException e) {
            throw new KettleException("Not able to write level cardinality file", e);
        } finally {
            MolapUtil.closeStreams(channel, fileOutputStream);
        }
    }

    /**
     * Below method will be used to update and write the slice meta data
     *
     * @throws KettleException
     */
    private void updateAndWriteSliceMetadataFile(String path) throws KettleException {
        File file = new File(path);
        String sliceMetaDataFilePath =
                file.getParentFile().getAbsolutePath() + File.separator + MolapUtil
                        .getSliceMetaDataFileName(meta.getCurrentRestructNumber());

        SliceMetaData sliceMetaData = new SliceMetaData();
        sliceMetaData.setDimensions(meta.getAggregateLevels());
        sliceMetaData.setActualDimensions(meta.getAggregateLevels());
        sliceMetaData.setMeasures(meta.getAggregateMeasuresColumnName());
        sliceMetaData.setActualDimLens(meta.getAggDimeLens());
        sliceMetaData.setDimLens(meta.getAggDimeLens());
        sliceMetaData.setMeasuresAggregator(this.aggregators);
        sliceMetaData.setHeirAnKeySize(meta.getHeirAndKeySize());
        sliceMetaData.setTableNamesToLoadMandatory(null);
        sliceMetaData.setKeyGenerator(KeyGeneratorFactory.getKeyGenerator(meta.getAggDimeLens()));
        MolapDataProcessorUtil.writeFileAsObjectStream(sliceMetaDataFilePath, sliceMetaData);
    }

    private void writeMeasureMetadataFile() throws KettleException {
        String metaDataFileName =
                MolapCommonConstants.MEASURE_METADATA_FILE_NAME + meta.getTabelName()
                        + MolapCommonConstants.MEASUREMETADATA_FILE_EXT;
        String measureMetaDataFileLocation = createStoreLocaion + metaDataFileName;

        try {
            MolapDataProcessorUtil
                    .writeMeasureMetaDataToFile(maxValue, minValue, decimalLength, uniqueValue,
                            aggType, new byte[minValue.length], measureMetaDataFileLocation);
        } catch (MolapDataProcessorException e) {
            SORTKEYSTEPLOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
        }
    }

    /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param smi The metadata to work with
     * @param sdi The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((MolapSortKeyAndGroupByStepMeta) smi);
        this.data = ((MolapSortKeyAndGroupByStepData) sdi);
        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((MolapSortKeyAndGroupByStepMeta) smi);
        this.data = ((MolapSortKeyAndGroupByStepData) sdi);
        this.molapSortKeys = null;
        super.dispose(smi, sdi);
        this.meta = null;
        this.data = null;
    }
}