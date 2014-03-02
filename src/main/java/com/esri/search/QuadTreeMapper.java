package com.esri.search;

import com.esri.hadoop.quadtree.PointData;
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
        implements Mapper<LongWritable, PointData, NullWritable, Text>
{
    private Text m_value = new Text();

    @Override
    public void map(
            final LongWritable key,
            final PointData pointData,
            final OutputCollector<NullWritable, Text> collector,
            final Reporter reporter) throws IOException
    {
        m_value.set(String.format("%.6f,%.6f", pointData.x, pointData.y));
        collector.collect(NullWritable.get(), m_value);
    }
}
