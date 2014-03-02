package com.esri.density;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class DensityMapper extends ShapefileMapper
{
    @Override
    protected void collect(
            final OutputCollector<IntWritable, IntWritable> collector,
            final Reporter reporter) throws IOException
    {
        collector.collect(m_origID, m_voyageId);
    }
}
