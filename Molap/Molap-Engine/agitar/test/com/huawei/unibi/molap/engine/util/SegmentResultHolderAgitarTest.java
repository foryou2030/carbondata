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

/**
 * Generated by Agitar build: AgitarOne Version 5.3.0.000022 (Build date: Jan 04, 2012) [5.3.0.000022]
 * JDK Version: 1.6.0_14
 *
 * Generated on 29 Jul, 2013 5:10:07 PM
 * Time to generate: 00:38.455 seconds
 *
 */

package com.huawei.unibi.molap.engine.util;

import com.agitar.lib.junit.AgitarTestCase;
import com.agitar.lib.mockingbird.Mockingbird;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MaxAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MinAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumAggregator;
import java.util.ArrayList;
import java.util.List;
import mondrian.rolap.SqlStatement;

public class SegmentResultHolderAgitarTest extends AgitarTestCase {
    
    public Class getTargetClass()  {
        return SegmentResultHolder.class;
    }
    
    public void testConstructor() throws Throwable {
        int[] dimIndexMap = new int[1];
        List dataTypes = new ArrayList(100);
        dataTypes.add(SqlStatement.Type.OBJECT);
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(dataTypes), dimIndexMap);
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        assertSame("segmentResultHolder.dimIndexMap", dimIndexMap, getPrivateField(segmentResultHolder, "dimIndexMap"));
        assertEquals("segmentResultHolder.next", -1, ((Number) getPrivateField(segmentResultHolder, "next")).intValue());
        assertEquals("segmentResultHolder.getDataTypes().size()", 1, segmentResultHolder.getDataTypes().size());
        assertEquals("segmentResultHolder.getDataTypes().size()", 1, segmentResultHolder.getDataTypes().size());
        assertNull("segmentResultHolder.result", getPrivateField(segmentResultHolder, "result"));
    }
    
    public void testConstructor1() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        assertSame("segmentResultHolder.dimIndexMap", dimIndexMap, getPrivateField(segmentResultHolder, "dimIndexMap"));
        assertEquals("segmentResultHolder.next", -1, ((Number) getPrivateField(segmentResultHolder, "next")).intValue());
        assertEquals("segmentResultHolder.getDataTypes().size()", 0, segmentResultHolder.getDataTypes().size());
        assertEquals("segmentResultHolder.getDataTypes().size()", 0, segmentResultHolder.getDataTypes().size());
        assertNull("segmentResultHolder.result", getPrivateField(segmentResultHolder, "result"));
    }
    
    public void testGetColumnCount() throws Throwable {
        SegmentResultHolder actual = (SegmentResultHolder) Mockingbird.getProxyObject(SegmentResultHolder.class);
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(actual, dimIndexMap);
        Mockingbird.enterRecordingMode();
        Mockingbird.setReturnValue(actual.getColumnCount(), 0);
        Mockingbird.enterTestMode(SegmentResultHolder.class);
        int result = segmentResultHolder.getColumnCount();
        assertEquals("result", 0, result);
    }
    
    public void testGetColumnCount1() throws Throwable {
        MolapResultHolder actual = new MolapResultHolder(new ArrayList(100));
        MeasureAggregator[] d = new MeasureAggregator[3];
        d[0] = new MinAggregator();
        d[1] = new MaxAggregator();
        d[2] = new SumAggregator();
        actual.createData(d);
        int[] dimIndexMap = new int[0];
        int[] dimIndexMap2 = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new SegmentResultHolder(actual, dimIndexMap), dimIndexMap2);
        int result = segmentResultHolder.getColumnCount();
        assertEquals("result", 3, result);
    }
    
    public void testIsNext() throws Throwable {
        MolapResultHolder actual = new MolapResultHolder(new ArrayList(100));
        MeasureAggregator[] d = new MeasureAggregator[1];
        d[0] = new AvgAggregator();
        actual.createData(d);
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(actual, dimIndexMap);
        boolean result = segmentResultHolder.isNext();
        assertEquals("segmentResultHolder.actual.next", 0, ((Number) getPrivateField(getPrivateField(segmentResultHolder, "actual"), "next")).intValue());
        assertTrue("result", result);
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
    }
    
    public void testIsNext1() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        boolean result = segmentResultHolder.isNext();
        assertEquals("segmentResultHolder.actual.next", 0, ((Number) getPrivateField(getPrivateField(segmentResultHolder, "actual"), "next")).intValue());
        assertFalse("result", result);
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
    }
    
    public void testReset() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        segmentResultHolder.reset();
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
    }
    
    public void testSetDataTypes() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(1000)), dimIndexMap);
        segmentResultHolder.setDataTypes(new ArrayList(100));
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
    }
    
    public void testSetObject() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        Object[][] data = new Object[3][];
        segmentResultHolder.setObject(data);
        int actual = ((MolapResultHolder) getPrivateField(segmentResultHolder, "actual")).getColumnCount();
        assertEquals("segmentResultHolder.actual.getColumnCount()", 3, actual);
        assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
    }
    
    public void testWasNull() throws Throwable {
        int[] dimIndexMap = new int[0];
        boolean result = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap).wasNull();
        assertFalse("result", result);
    }
    
    public void testConstructorThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[3];
        dimIndexMap[1] = -2;
        List dataTypes = new ArrayList(100);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.LONG);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.LONG);
        dataTypes.add(SqlStatement.Type.INT);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.INT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.INT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.INT);
        dataTypes.add(SqlStatement.Type.LONG);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.INT);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.STRING);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.DOUBLE);
        dataTypes.add(SqlStatement.Type.OBJECT);
        dataTypes.add(SqlStatement.Type.LONG);
        MolapResultHolder actual = new MolapResultHolder(dataTypes);
        try {
            new SegmentResultHolder(actual, dimIndexMap);
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "-2", ex.getMessage());
            assertThrownBy(ArrayList.class, ex);
        }
    }
    
    public void testConstructorThrowsIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        int[] dimIndexMap2 = new int[0];
        MolapResultHolder actual = new SegmentResultHolder(new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap), dimIndexMap2);
        int[] dimIndexMap3 = new int[1];
        try {
            new SegmentResultHolder(actual, dimIndexMap3);
            fail("Expected IndexOutOfBoundsException to be thrown");
        } catch (IndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "Index: 0, Size: 0", ex.getMessage());
            assertThrownBy(ArrayList.class, ex);
        }
    }
    
    public void testConstructorThrowsNullPointerException() throws Throwable {
        MolapResultHolder actual = new MolapResultHolder(new ArrayList(100));
        try {
            new SegmentResultHolder(actual, null);
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
        }
    }
    
    public void testGetColumnCountThrowsNullPointerException() throws Throwable {
        int[] dimIndexMap = new int[0];
        int[] dimIndexMap2 = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap), dimIndexMap2);
        try {
            segmentResultHolder.getColumnCount();
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(MolapResultHolder.class, ex);
        }
    }
    
    public void testGetDataTypeThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder((MolapResultHolder) Mockingbird.getProxyObject(MolapResultHolder.class), dimIndexMap);
        Mockingbird.enterTestMode(SegmentResultHolder.class);
        try {
            segmentResultHolder.getDataType(-1);
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "-1", ex.getMessage());
            assertThrownBy(ArrayList.class, ex);
            assertEquals("segmentResultHolder.getDataTypes().size()", 0, segmentResultHolder.getDataTypes().size());
        }
    }
    
    public void testGetDataTypeThrowsIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        try {
            segmentResultHolder.getDataType(100);
            fail("Expected IndexOutOfBoundsException to be thrown");
        } catch (IndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "Index: 100, Size: 0", ex.getMessage());
            assertThrownBy(ArrayList.class, ex);
            assertEquals("segmentResultHolder.getDataTypes().size()", 0, segmentResultHolder.getDataTypes().size());
        }
    }
    
    public void testGetDoubleThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        try {
            segmentResultHolder.getDouble(100);
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "99", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        }
    }
    
    public void testGetIntThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        try {
            segmentResultHolder.getInt(100);
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "99", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        }
    }
    
    public void testGetLongThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        try {
            segmentResultHolder.getLong(100);
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "99", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        }
    }
    
    public void testGetObjectThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap);
        try {
            segmentResultHolder.getObject(100);
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            assertEquals("ex.getMessage()", "99", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        }
    }
    
    public void testIsNextThrowsArrayIndexOutOfBoundsException() throws Throwable {
        int[] dimIndexMap = new int[0];
        Object[][] data = new Object[0][];
        int[] dimIndexMap2 = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(new SegmentResultHolder(new MolapResultHolder(new ArrayList(100)), dimIndexMap), dimIndexMap2);
        segmentResultHolder.setObject(data);
        try {
            segmentResultHolder.isNext();
            fail("Expected ArrayIndexOutOfBoundsException to be thrown");
        } catch (ArrayIndexOutOfBoundsException ex) {
            int actual = ((Number) getPrivateField(getPrivateField(getPrivateField(segmentResultHolder, "actual"), "actual"), "next")).intValue();
            assertEquals("segmentResultHolder.actual.actual.next", 0, actual);
            assertEquals("ex.getMessage()", "0", ex.getMessage());
            assertThrownBy(MolapResultHolder.class, ex);
            assertFalse("segmentResultHolder.wasNull()", segmentResultHolder.wasNull());
        }
    }
    
    public void testIsNextThrowsNullPointerException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(null, dimIndexMap);
        try {
            segmentResultHolder.isNext();
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertNull("segmentResultHolder.actual", getPrivateField(segmentResultHolder, "actual"));
        }
    }
    
    public void testResetThrowsNullPointerException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(null, dimIndexMap);
        try {
            segmentResultHolder.reset();
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertNull("segmentResultHolder.actual", getPrivateField(segmentResultHolder, "actual"));
        }
    }
    
    public void testSetDataTypesThrowsNullPointerException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(null, dimIndexMap);
        try {
            segmentResultHolder.setDataTypes(new ArrayList(100));
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertNull("segmentResultHolder.actual", getPrivateField(segmentResultHolder, "actual"));
        }
    }
    
    public void testSetObjectThrowsNullPointerException() throws Throwable {
        int[] dimIndexMap = new int[0];
        SegmentResultHolder segmentResultHolder = new SegmentResultHolder(null, dimIndexMap);
        Object[][] data = new Object[3][];
        try {
            segmentResultHolder.setObject(data);
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
            assertNull("segmentResultHolder.actual", getPrivateField(segmentResultHolder, "actual"));
        }
    }
    
    public void testWasNullThrowsNullPointerException() throws Throwable {
        int[] dimIndexMap = new int[0];
        try {
            new SegmentResultHolder(null, dimIndexMap).wasNull();
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(SegmentResultHolder.class, ex);
        }
    }
}

