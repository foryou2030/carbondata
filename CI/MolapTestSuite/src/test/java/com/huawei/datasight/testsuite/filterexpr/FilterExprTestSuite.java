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

package com.huawei.datasight.testsuite.filterexpr;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite from where all the TestCases for filter expression query support will be triggered
 * @author N00902756
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	AllDataTypesTestCase.class, 
	IntegerDataTypeTestCase.class, 
	StringDataTypeTestCase.class, 
	TimestampDataTypeTestCase.class, 
	NumericDataTypeTestCase.class
})
public class FilterExprTestSuite {

}
