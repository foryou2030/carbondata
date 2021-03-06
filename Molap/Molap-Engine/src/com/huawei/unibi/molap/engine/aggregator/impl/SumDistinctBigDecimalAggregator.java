package com.huawei.unibi.molap.engine.aggregator.impl;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.util.DataTypeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author K00900207
 * 
 *         <p>
 *         The sum distinct aggregator
 *         <p>
 *         Ex:
 *         <p>
 *         ID NAME Sales
 *         <p>
 *         1 a 200
 *         <p>
 *         2 a 100
 *         <p>
 *         3 a 200
 *         <p>
 *         select sum(distinct sales) # would result 300
 */
public class SumDistinctBigDecimalAggregator extends AbstractMeasureAggregatorBasic
{

    /**
     *
     */
    private static final long serialVersionUID = 6313463368629960155L;

    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the
     * Aggregators. There is no aggregation expected after setting this value.
     */
    private BigDecimal computedFixedValue;

    /**
     *
     */
    private Set<BigDecimal> valueSet;

    public SumDistinctBigDecimalAggregator()
    {
        valueSet = new HashSet<BigDecimal>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * Distinct Aggregate function which update the Distinct set
     *
     * @param newVal
     *            new value
     *
     */
    @Override
    public void agg(Object newVal)
    {
        valueSet.add(newVal instanceof BigDecimal ? (BigDecimal)newVal : new BigDecimal(newVal.toString()));
    }

    @Override
    public void agg(MolapReadDataHolder newVal, int index)
    {
        BigDecimal valueBigDecimal = newVal.getReadableBigDecimalValueByIndex(index);
        valueSet.add(valueBigDecimal);
    }
    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
        Iterator<BigDecimal> iterator = valueSet.iterator();
        ByteBuffer buffer = ByteBuffer.allocate(valueSet.size() * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        // CHECKSTYLE:OFF Approval No:Approval-V3R8C00_018
        while(iterator.hasNext())
        { // CHECKSTYLE:ON
//            byte[] bytes = iterator.next().toString().getBytes();
            byte[] bytes = DataTypeUtil.bigDecimalToByte(iterator.next());
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
        buffer.rewind();
        return buffer.array();
    }

    private void agg(Set<BigDecimal> set2)
    {
        valueSet.addAll(set2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        SumDistinctBigDecimalAggregator distinctAggregator = (SumDistinctBigDecimalAggregator)aggregator;
        agg(distinctAggregator.valueSet);
    }

    @Override
    public BigDecimal getBigDecimalValue()
    {
        if(computedFixedValue == null)
        {
            BigDecimal result = new BigDecimal(0);
            for(BigDecimal aValue : valueSet)
            {
                result = result.add(aValue);
            }
            return result;
        }
        return computedFixedValue;
    }
    @Override
    public Object getValueObject()
    {
        return getBigDecimalValue();
    }

    /**
     *
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(Object)
     *
     */
    @Override
    public void setNewValue(Object newValue)
    {
        computedFixedValue = (BigDecimal)newValue;
        valueSet = null;
    }

    @Override
    public boolean isFirstTime()
    {
        return false;
    }

    @Override
    public void writeData(DataOutput dataOutput) throws IOException
    {
        if(computedFixedValue != null)
        {
//            byte[] bytes = computedFixedValue.toString().getBytes();
            byte[] bytes = DataTypeUtil.bigDecimalToByte(computedFixedValue);
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + bytes.length);
            byteBuffer.putInt(-1);
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            dataOutput.write(byteBuffer.array());
        }
        else
        {
            int length = valueSet.size() * 8 + valueSet.size() * 4;
            ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4 + 1);
            byteBuffer.putInt(length);
            for(BigDecimal val : valueSet)
            {
                byte[] bytes = val.toString().getBytes();
                byteBuffer.putInt(-1);
                byteBuffer.putInt(bytes.length);
                byteBuffer.put(bytes);
            }
            byteBuffer.flip();
            dataOutput.write(byteBuffer.array());
        }
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        int length = inPut.readInt();

        if(length == -1)
        {
            computedFixedValue = new BigDecimal(inPut.readUTF());
            valueSet = null;
        }
        else
        {
            length = length / 8;
            valueSet = new HashSet<BigDecimal>(length + 1, 1.0f);
            for(int i = 0;i < length;i++)
            {
                valueSet.add(new BigDecimal(inPut.readUTF()));
            }
        }
    }

    @Override
    public void merge(byte[] value)
    {
        if(0 == value.length)
        {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.rewind();
        while(buffer.hasRemaining())
        {
            byte[] valueByte = new byte[buffer.getInt()];
            buffer.get(valueByte);
//            BigDecimal valueBigDecimal = new BigDecimal(new String(valueByte));
            BigDecimal valueBigDecimal = DataTypeUtil.byteToBigDecimal(valueByte);
            agg(valueBigDecimal);
        }
    }

    public String toString()
    {
        if(computedFixedValue == null)
        {
            return valueSet.size() + "";
        }
        return computedFixedValue + "";
    }

    @Override
    public MeasureAggregator getCopy()
    {
        SumDistinctBigDecimalAggregator aggregator = new SumDistinctBigDecimalAggregator();
        aggregator.valueSet = new HashSet<BigDecimal>(valueSet);
        return aggregator;
    }

    @Override
    public int compareTo(MeasureAggregator msr)
    {
        BigDecimal msrValObj = getBigDecimalValue();
        BigDecimal otherVal = msr.getBigDecimalValue();

        return msrValObj.compareTo(otherVal);
    }
}
