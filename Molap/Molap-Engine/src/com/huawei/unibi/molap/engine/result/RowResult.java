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

package com.huawei.unibi.molap.engine.result;

import com.huawei.unibi.molap.engine.scanner.impl.MolapKey;
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue;

public class RowResult
{
    private MolapKey key;
    
    private MolapValue value;

    public MolapKey getKey()
    {
        return key;
    }

    public void setKey(MolapKey key)
    {
        this.key = key;
    }

    public MolapValue getValue()
    {
        return value;
    }

    public void setValue(MolapValue value)
    {
        this.value = value;
    }
}
