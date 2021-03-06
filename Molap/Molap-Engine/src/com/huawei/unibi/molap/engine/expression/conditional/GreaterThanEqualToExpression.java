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

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class GreaterThanEqualToExpression extends BinaryConditionalExpression
{
    private static final long serialVersionUID = 4185317066280688984L;

    public GreaterThanEqualToExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult elRes = left.evaluate(value);
        ExpressionResult erRes = right.evaluate(value);
        ExpressionResult exprResVal1 = elRes;   
        if(elRes.isNull() || erRes.isNull()){
            elRes.set(DataType.BooleanType, false);
            return elRes;
        }
        if(elRes.getDataType() != erRes.getDataType())
        {
            if(elRes.getDataType().getPresedenceOrder() < erRes.getDataType().getPresedenceOrder())
            {
                exprResVal1 = erRes;
            }
            
        }
        boolean result = false;
        switch(exprResVal1.getDataType())
        {
        case StringType:
            result = elRes.getString().compareTo(erRes.getString())>=0;
            break;
        case IntegerType:
            result = elRes.getInt()>=(erRes.getInt());
            break;
        case DoubleType:
            result = elRes.getDouble()>=(erRes.getDouble());
            break;
        case TimestampType:
            result = elRes.getTime()>=(erRes.getTime());
            break;
        case LongType:
            result = elRes.getLong()>= (erRes.getLong());
            break;
        case DecimalType:
            result = elRes.getDecimal().compareTo(erRes.getDecimal()) >= 0;
            break;
        default:
            break;
        }
        exprResVal1.set(DataType.BooleanType, result);
        return exprResVal1;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.GREATERYHAN_EQUALTO;
    }

    @Override
    public String getString()
    {
        return "GreaterThanEqualTo(" + left.getString() + ',' + right.getString() + ')';
    }
}
