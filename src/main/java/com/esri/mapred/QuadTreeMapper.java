package com.esri.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class QuadTreeMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, NullWritable, Text>
{
    @Override
    public void map(
            final LongWritable key,
            final Text value,
            final OutputCollector<NullWritable, Text> collector,
            final Reporter reporter) throws IOException
    {
        collector.collect(NullWritable.get(), value);
    }
}
