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

package com.huawei.unibi.molap.store.writer;

public class NodeHolder {
    /**
     * keyArray
     */
    private byte[] keyArray;

    /**
     * dataArray
     */
    private byte[] dataArray;

    /**
     * measureLenght
     */
    private int[] measureLenght;

    /**
     * startKey
     */
    private byte[] startKey;

    /**
     * endKey
     */
    private byte[] endKey;

    /**
     * entryCount
     */
    private int entryCount;
    /**
     * keyLenghts
     */
    private int[] keyLengths;

    /**
     * dataAfterCompression
     */
    private short[][] dataAfterCompression;

    /**
     * indexMap
     */
    private short[][] indexMap;

    /**
     * keyIndexBlockLenght
     */
    private int[] keyBlockIndexLength;

    /**
     * isSortedKeyBlock
     */
    private boolean[] isSortedKeyBlock;

    private byte[][] compressedIndex;

    private byte[][] compressedIndexMap;

    /**
     * dataIndexMap
     */
    private int[] dataIndexMapLength;

    /**
     * dataIndexMap
     */
    private int[] dataIndexMapOffsets;

    /**
     * compressedDataIndex
     */
    private byte[][] compressedDataIndex;

    /**
     * column min max data
     */
    private byte[][] columnMinMaxData;

    /**
     * @return the keyArray
     */
    public byte[] getKeyArray() {
        return keyArray;
    }

    /**
     * @param keyArray the keyArray to set
     */
    public void setKeyArray(byte[] keyArray) {
        this.keyArray = keyArray;
    }

    /**
     * @return the dataArray
     */
    public byte[] getDataArray() {
        return dataArray;
    }

    /**
     * @param dataArray the dataArray to set
     */
    public void setDataArray(byte[] dataArray) {
        this.dataArray = dataArray;
    }

    /**
     * @return the measureLenght
     */
    public int[] getMeasureLenght() {
        return measureLenght;
    }

    /**
     * @param measureLenght the measureLenght to set
     */
    public void setMeasureLenght(int[] measureLenght) {
        this.measureLenght = measureLenght;
    }

    /**
     * @return the startKey
     */
    public byte[] getStartKey() {
        return startKey;
    }

    /**
     * @param startKey the startKey to set
     */
    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    /**
     * @return the endKey
     */
    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * @param endKey the endKey to set
     */
    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    /**
     * @return the entryCount
     */
    public int getEntryCount() {
        return entryCount;
    }

    /**
     * @param entryCount the entryCount to set
     */
    public void setEntryCount(int entryCount) {
        this.entryCount = entryCount;
    }

    /**
     * @return the keyLenghts
     */
    public int[] getKeyLengths() {
        return keyLengths;
    }

    public void setKeyLengths(int[] keyLengths) {
        this.keyLengths = keyLengths;
    }

    /**
     * @return the dataAfterCompression
     */
    public short[][] getDataAfterCompression() {
        return dataAfterCompression;
    }

    /**
     * @param dataAfterCompression the dataAfterCompression to set
     */
    public void setDataAfterCompression(short[][] dataAfterCompression) {
        this.dataAfterCompression = dataAfterCompression;
    }

    /**
     * @return the indexMap
     */
    public short[][] getIndexMap() {
        return indexMap;
    }

    /**
     * @param indexMap the indexMap to set
     */
    public void setIndexMap(short[][] indexMap) {
        this.indexMap = indexMap;
    }

    /**
     * @return the keyBlockIndexLength
     */
    public int[] getKeyBlockIndexLength() {
        return keyBlockIndexLength;
    }

    /**
     * @param keyBlockIndexLength the keyBlockIndexLength to set
     */
    public void setKeyBlockIndexLength(int[] keyBlockIndexLength) {
        this.keyBlockIndexLength = keyBlockIndexLength;
    }

    /**
     * @return the isSortedKeyBlock
     */
    public boolean[] getIsSortedKeyBlock() {
        return isSortedKeyBlock;
    }

    /**
     * @param isSortedKeyBlock the isSortedKeyBlock to set
     */
    public void setIsSortedKeyBlock(boolean[] isSortedKeyBlock) {
        this.isSortedKeyBlock = isSortedKeyBlock;
    }

    /**
     * @return the compressedIndexex
     */
    public byte[][] getCompressedIndex() {
        return compressedIndex;
    }

    public void setCompressedIndex(byte[][] compressedIndex) {
        this.compressedIndex = compressedIndex;
    }

    /**
     * @return the compressedIndexMap
     */
    public byte[][] getCompressedIndexMap() {
        return compressedIndexMap;
    }

    /**
     * @param compressedIndexMap the compressedIndexMap to set
     */
    public void setCompressedIndexMap(byte[][] compressedIndexMap) {
        this.compressedIndexMap = compressedIndexMap;
    }

    /**
     * @return the compressedDataIndex
     */
    public byte[][] getCompressedDataIndex() {
        return compressedDataIndex;
    }

    /**
     * @param compressedDataIndex the compressedDataIndex to set
     */
    public void setCompressedDataIndex(byte[][] compressedDataIndex) {
        this.compressedDataIndex = compressedDataIndex;
    }

    /**
     * @return the dataIndexMapOffsets
     */
    public int[] getDataIndexMapOffsets() {
        return dataIndexMapOffsets;
    }

    /**
     * @param dataIndexMapOffsets the dataIndexMapOffsets to set
     */
    public void setDataIndexMapOffsets(int[] dataIndexMapOffsets) {
        this.dataIndexMapOffsets = dataIndexMapOffsets;
    }

    /**
     * @return the dataIndexMapLength
     */
    public int[] getDataIndexMapLength() {
        return dataIndexMapLength;
    }

    /**
     * @param dataIndexMapLength the dataIndexMapLength to set
     */
    public void setDataIndexMapLength(int[] dataIndexMapLength) {
        this.dataIndexMapLength = dataIndexMapLength;
    }

    public byte[][] getColumnMinMaxData() {
        return this.columnMinMaxData;
    }

    public void setColumnMinMaxData(byte[][] columnMinMaxData) {
        this.columnMinMaxData = columnMinMaxData;

    }

}
