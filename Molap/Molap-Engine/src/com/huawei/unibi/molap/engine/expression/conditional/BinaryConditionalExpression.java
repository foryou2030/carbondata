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

package com.huawei.unibi.molap.engine.expression.conditional;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.expression.ColumnExpression;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.logical.BinaryLogicalExpression;

public abstract class BinaryConditionalExpression  extends BinaryLogicalExpression implements ConditionalExpression
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public BinaryConditionalExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

    // Will get the column informations involved in the expressions by
    // traversing the tree
    public List<ColumnExpression> getColumnList()
    {
        // TODO
        List<ColumnExpression> listOfExp=new ArrayList<ColumnExpression>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        getColumnList(this,listOfExp);
        return listOfExp;
    }
    
    private void getColumnList(Expression expression, List<ColumnExpression> lst)
    {
        if(expression instanceof ColumnExpression)
        {
            ColumnExpression colExp=(ColumnExpression)expression;
            boolean found=false;
                
           for(ColumnExpression currentColExp:lst)
            {
                if(currentColExp.getColumnName().equals(colExp.getColumnName()))
                {
                    found=true;
                    colExp.setColIndex(currentColExp.getColIndex());
                    break;
                }
            }
            if(!found)
            {
                colExp.setColIndex(lst.size());
                lst.add(colExp);
            }  
        }
        for(Expression child: expression.getChildren()){
           getColumnList(child, lst);
        }
    }

    public boolean isSingleDimension()
    {
       List<ColumnExpression> listOfExp = new ArrayList<ColumnExpression>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        getColumnList(this, listOfExp);
        if(listOfExp.size() == 1 && listOfExp.get(0).isDimension())
        {
            return true;
        }
        return false;

    }

}
