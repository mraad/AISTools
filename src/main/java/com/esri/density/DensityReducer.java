package com.esri.density;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 */
public class DensityReducer
        extends MapReduceBase
        implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
    private IntWritable m_val = new IntWritable();

    @Override
    public void reduce(
            final IntWritable origID,
            final Iterator<IntWritable> iterator,
            final OutputCollector<IntWritable, IntWritable> collector,
            final Reporter reporter) throws IOException
    {
        final Set<Integer> set = new HashSet<Integer>();
        while (iterator.hasNext())
        {
            set.add(iterator.next().get());
        }
        m_val.set(set.size());
        collector.collect(origID, m_val);
    }
}
