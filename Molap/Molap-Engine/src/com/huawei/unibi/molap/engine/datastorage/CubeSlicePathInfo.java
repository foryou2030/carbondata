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

package com.huawei.unibi.molap.engine.datastorage;

import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;

/**
 *
 * Class that deduces the info from Load path supplied
 *
 */
public class CubeSlicePathInfo
{
    /**
     * 
     */
    private String loadPath;
    
    /**
     * 
     */
//    private String rsFolder;
    
    /**
     * 
     */
//    private String loadFolder;
    
    /**
     * 
     */
//    private String rsPath;
    
    /**
     * 
     */
//    private String cubeName;
//
//    /**
//     * 
//     */
////    private String tableName;
//    
//    /**
//     * 
//     */
//    private String schemaName;
    
    /**
     * 
     */
//    private String cubeUniqueName;
    
    /**
     * 
     * Comment for <code>logger</code>
     * Comment for <code>LOGGER</code>
     * 
     */
  //  private static final LogService LOGGER = LogServiceFactory.getLogService(CubeSlicePathInfo.class.getName());

    /**
     * @param loadPath
     */
    public CubeSlicePathInfo(String loadPath)
    {
       formInfo(loadPath);
    }

    /**
     * @param loadFolderPath
     */
    private void formInfo(String loadFolderPath)
    {
        //
        MolapFile loadPathFolder = FileFactory.getMolapFile(loadFolderPath, FileFactory.getFileType(loadFolderPath));
        loadPath=loadPathFolder.getCanonicalPath();
//        loadFolder = loadPathFolder.getName();
//        MolapFile tableFolder = loadPathFolder.getParentFile();
//        tableName = tableFolder.getName();
//        MolapFile rsPathFolder = tableFolder.getParentFile();
        //
//        rsFolder = rsPathFolder.getName();
//        rsPath = rsPathFolder.getPath();
//        MolapFile cubeFolder = rsPathFolder.getParentFile();
//        cubeName = cubeFolder.getName();
//        schemaName = cubeFolder.getParentFile().getName();
//        cubeUniqueName=schemaName+'_'+cubeName;
    }

    /**
     * @return
     */
    public String getLoadPath()
    {
        return loadPath;
    }

    /**
     * @return
     */
    /*public String getRsFolder()
    {
        return rsFolder;
    }

    *//**
     * @return
     *//*
    public String getLoadFolder()
    {
        return loadFolder;
    }
    
    *//**
     * @return
     *//*
    public String getRsPath()
    {
        return rsPath;
    }

    *//**
     * @return
     *//*
    public String getCubeName()
    {
        return cubeName;
    }

    *//**
     * @return
     *//*
    public String getTableName()
    {
        return tableName;
    }
    
    *//**
     * @return
     *//*
    public String getSchemaName()
    {
        return schemaName;
    }

    *//**
     * getCubeUniqueName
     * @return String
     *//*
    public String getCubeUniqueName()
    {
        return cubeUniqueName;
    }*/
    
}
