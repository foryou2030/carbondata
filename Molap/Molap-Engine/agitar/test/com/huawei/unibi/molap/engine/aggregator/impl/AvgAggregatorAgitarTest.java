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
 * Generated on 29 Jul, 2013 4:48:26 PM
 * Time to generate: 00:13.537 seconds
 *
 */

package com.huawei.unibi.molap.engine.aggregator.impl;

import com.agitar.lib.junit.AgitarTestCase;

public class AvgAggregatorAgitarTest extends AgitarTestCase {
    
    public Class getTargetClass()  {
        return AvgAggregator.class;
    }
    
    public void testConstructor() throws Throwable {
        new AvgAggregator();
        assertTrue("Test call resulted in expected outcome", true);
    }
    
    public void testAgg() throws Throwable {
        AvgAggregator avgOfAvgAggregator = new AvgOfAvgAggregator();
        byte[] key = new byte[2];
        avgOfAvgAggregator.agg(100.0, key, 100, 1000);
        assertEquals("(AvgOfAvgAggregator) avgOfAvgAggregator.count", 1.0, ((AvgOfAvgAggregator) avgOfAvgAggregator).count, 1.0E-6);
        assertEquals("(AvgOfAvgAggregator) avgOfAvgAggregator.aggVal", 100.0, ((AvgOfAvgAggregator) avgOfAvgAggregator).aggVal, 1.0E-6);
    }
    
    public void testAgg1() throws Throwable {
        AvgAggregator avgAggregator = new AvgAggregator();
        avgAggregator.agg(100.0, 1000.0);
        assertEquals("avgAggregator.count", 1.0, avgAggregator.count, 1.0E-6);
        assertEquals("avgAggregator.aggVal", 100.0, avgAggregator.aggVal, 1.0E-6);
    }
    
    public void testGetValue() throws Throwable {
        new AvgAggregator().getValue();
        assertTrue("Test call resulted in expected outcome", true);
    }
    
    public void testGetValue1() throws Throwable {
        AvgAggregator avgAggregator = new AvgAggregator();
        avgAggregator.agg(0.0, 100.0);
        double result = avgAggregator.getValue();
        assertEquals("result", 0.0, result, 1.0E-6);
    }
    
    public void testGetValueObject() throws Throwable {
        new AvgAggregator().getValueObject();
        assertTrue("Test call resulted in expected outcome", true);
    }
    
    public void testGetValueObject1() throws Throwable {
        byte[] key = new byte[0];
        AvgAggregator avgOfAvgAggregator = new AvgOfAvgAggregator();
        avgOfAvgAggregator.agg(0.0, key, 100, 1000);
        Double result = (Double) avgOfAvgAggregator.getValueObject();
        assertEquals("result", 0.0, result.doubleValue(), 1.0E-6);
    }
    
    public void testMerge() throws Throwable {
        AvgAggregator avgAggregator = new AvgAggregator();
        avgAggregator.merge(new AvgAggregator());
        assertEquals("avgAggregator.count", 0.0, avgAggregator.count, 1.0E-6);
        assertEquals("avgAggregator.aggVal", 0.0, avgAggregator.aggVal, 1.0E-6);
    }
    
    public void testMergeThrowsClassCastException() throws Throwable {
        AvgAggregator avgAggregator = new AvgAggregator();
        try {
            avgAggregator.merge(new DistinctCountAggregator());
            fail("Expected ClassCastException to be thrown");
        } catch (ClassCastException ex) {
            assertEquals("ex.getClass()", ClassCastException.class, ex.getClass());
            assertThrownBy(AvgAggregator.class, ex);
            assertEquals("avgAggregator.count", 0.0, avgAggregator.count, 1.0E-6);
            assertEquals("avgAggregator.aggVal", 0.0, avgAggregator.aggVal, 1.0E-6);
        }
    }
    
    public void testMergeThrowsNullPointerException() throws Throwable {
        AvgAggregator avgOfAvgAggregator = new AvgOfAvgAggregator();
        try {
            avgOfAvgAggregator.merge(null);
            fail("Expected NullPointerException to be thrown");
        } catch (NullPointerException ex) {
            assertNull("ex.getMessage()", ex.getMessage());
            assertThrownBy(AvgAggregator.class, ex);
            assertEquals("(AvgOfAvgAggregator) avgOfAvgAggregator.count", 0.0, ((AvgOfAvgAggregator) avgOfAvgAggregator).count, 1.0E-6);
            assertEquals("(AvgOfAvgAggregator) avgOfAvgAggregator.aggVal", 0.0, ((AvgOfAvgAggregator) avgOfAvgAggregator).aggVal, 1.0E-6);
        }
    }
}

