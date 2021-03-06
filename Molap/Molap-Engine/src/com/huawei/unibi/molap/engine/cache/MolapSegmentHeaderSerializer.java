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

package com.huawei.unibi.molap.engine.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Class responsible to serialize and deserialize from and to JDBM DB
 *
 */
public class MolapSegmentHeaderSerializer implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 2958864791162240071L;

    public MolapSegmentHeader deserialize(DataInput in) throws IOException, ClassNotFoundException
    {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream stream = new ObjectInputStream(arrayInputStream);
        MolapSegmentHeader header = (MolapSegmentHeader)stream.readObject();
        return header;
    }

    public void serialize(DataOutput out, MolapSegmentHeader header) throws IOException
    {
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(arrayOutputStream);
        stream.writeObject(header);
        byte[] byteArray = arrayOutputStream.toByteArray();
        out.writeInt(byteArray.length);
        out.write(byteArray);

    }
   
}
