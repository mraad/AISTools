package com.esri.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class QuadTreeFileInputFormat
        extends FileInputFormat<LongWritable, Text>
{
    @Override
    protected boolean isSplitable(
            final FileSystem fs,
            final Path path)
    {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException
    {
        return new QuadTreeRecordReader(inputSplit, jobConf);
    }
}
