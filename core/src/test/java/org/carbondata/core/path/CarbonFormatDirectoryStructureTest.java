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

package org.carbondata.core.path;

import java.io.IOException;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * Test carbon directory structure
 */
public class CarbonFormatDirectoryStructureTest {

    private final String CARBON_STORE = "/opt/carbonstore";

    /**
     * test table path methods
     */
    @Test public void testTablePathStructure() throws IOException {
        CarbonTableIdentifier tableIdentifier = new CarbonTableIdentifier("d1", "t1");
        CarbonStorePath carbonStorePath = new CarbonStorePath(CARBON_STORE);
        CarbonTablePath carbonTablePath = carbonStorePath.getCarbonTablePath(tableIdentifier);
        assertTrue(carbonTablePath.getPath().equals(CARBON_STORE + "/d1/t1"));
        assertTrue(carbonTablePath.getSchemaFilePath().equals(CARBON_STORE + "/d1/t1/Metadata/schema"));
        assertTrue(carbonTablePath.getTableStatusFilePath()
                .equals(CARBON_STORE + "/d1/t1/Metadata/tablestatus"));
        assertTrue(carbonTablePath.getDictionaryFilePath("t1_c1")
                .equals(CARBON_STORE + "/d1/t1/Metadata/t1_c1.dict"));
        assertTrue(carbonTablePath.getDictionaryMetaFilePath("t1_c1")
                .equals(CARBON_STORE + "/d1/t1/Metadata/t1_c1.dictmeta"));
        assertTrue(carbonTablePath.getSortIndexFilePath("t1_c1")
                .equals(CARBON_STORE + "/d1/t1/Metadata/t1_c1.sortindex"));
        assertTrue(carbonTablePath.getCarbonDataFilePath("1", 2, 3, 4, "999")
                .equals(CARBON_STORE + "/d1/t1/Fact/Part1/Segment2/part-3-4-999.carbondata"));
    }

    /**
     * test data file name
     */
    @Test public void testDataFileName() throws IOException {
        assertTrue(CarbonTablePath.DataFileUtil.getPartNo("part-3-4-999.carbondata").equals("3"));
        assertTrue(CarbonTablePath.DataFileUtil.getTaskNo("part-3-4-999.carbondata").equals("4"));
        assertTrue(CarbonTablePath.DataFileUtil.getUpdateTimeStamp("part-3-4-999.carbondata")
                .equals("999"));
    }
}
