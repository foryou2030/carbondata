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

package com.huawei.unibi.molap.engine.mondrian.extensions;


public class MolapDummyDialect //implements Dialect
{

//    @Override
//    public String toUpper(String expr)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    public String caseWhenElse(String cond, String thenExpr, String elseExpr,boolean quoteThenExpr, boolean quoteElseExpr)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String quoteIdentifier(String val)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public void quoteIdentifier(String val, StringBuilder buf)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public String quoteIdentifier(String qual, String name)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public void quoteIdentifier(StringBuilder buf, String... names)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public String getQuoteIdentifierString()
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public void quoteStringLiteral(StringBuilder buf, String s)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void quoteNumericLiteral(StringBuilder buf, String value)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void quoteBooleanLiteral(StringBuilder buf, String value)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void quoteDateLiteral(StringBuilder buf, String value)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void quoteTimeLiteral(StringBuilder buf, String value)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void quoteTimestampLiteral(StringBuilder buf, String value)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean requiresAliasForFromQuery()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsAs()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsFromQuery()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsCompoundCountDistinct()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsCountDistinct()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsMultipleCountDistinct()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsMultipleDistinctSqlMeasures()
//    {
//        // TODO Auto-generated method stub
//        return true;
//    }
//
//    @Override
//    public String generateInline(List<String> columnNames, List<String> columnTypes, List<String[]> valueList)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public boolean needsExponent(Object value, String valueString)
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public void quote(StringBuilder buf, Object value, Datatype datatype)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean allowsDdl()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public String generateOrderItem(String expr, boolean nullable, boolean ascending, boolean collateNullsLast)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public boolean supportsGroupByExpressions()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean supportsGroupingSets()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean supportsUnlimitedValueList()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean requiresGroupByAlias()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean requiresOrderByAlias()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean requiresHavingAlias()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsOrderByAlias()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean requiresUnionOrderByOrdinal()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean requiresUnionOrderByExprToBeInSelectClause()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean supportsMultiValueInExpr()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean supportsResultSetConcurrency(int type, int concurrency)
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public int getMaxColumnNameLength()
//    {
//        // TODO Auto-generated method stub
//        return 0;
//    }
//
//    @Override
//    public DatabaseProduct getDatabaseProduct()
//    {
//
//        return DatabaseProduct.UNKNOWN;
//    }
//
//    @Override
//    public void appendHintsAfterFromClause(StringBuilder buf, Map<String, String> hints)
//    {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public boolean allowsDialectSharing()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsSelectNotInGroupBy()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsJoinOn()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public boolean allowsRegularExpressionInWhereClause()
//    {
//        // TODO Auto-generated method stub
//        return false;
//    }
//
//    @Override
//    public String generateCountExpression(String exp)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String generateRegularExpression(String source, String javaRegExp)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public String caseWhenElse(String arg0, String arg1, String arg2)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }

}
