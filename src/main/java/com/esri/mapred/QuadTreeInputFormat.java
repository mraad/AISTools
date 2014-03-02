package com.esri.mapred;

import com.esri.hadoop.quadtree.PointData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class QuadTreeInputFormat
        extends FileInputFormat<LongWritable, PointData>
{
    @Override
    protected boolean isSplitable(
            final FileSystem fs,
            final Path path)
    {
        return false;
    }

    @Override
    public RecordReader<LongWritable, PointData> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException
    {
        return new QuadTreeRecordReader(inputSplit, jobConf);
    }
}
