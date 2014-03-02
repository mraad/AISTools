package com.esri.mapred;

import com.esri.Const;
import com.esri.hadoop.Extent;
import com.esri.hadoop.quadtree.PointData;
import com.esri.hadoop.quadtree.QuadTree;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 */
public class AISRecordReader
        implements RecordReader<LongWritable, Text>
{
    private Logger m_logger = LoggerFactory.getLogger(getClass());
    private Iterator<PointData> m_iterator;
    private FSDataInputStream m_indexStream;
    private FSDataInputStream m_dataStream;
    private ByteArrayOutputStream m_byteArrayOutputStream = new ByteArrayOutputStream();
    private boolean m_hasNext = true;

    public AISRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf) throws IOException
    {
        if (inputSplit instanceof FileSplit)
        {
            final String replaceIndex = jobConf.get(Const.PATH_INDEX, "/ais-index");
            final String replaceData = jobConf.get(Const.PATH_DATA, "/ais");

            final FileSystem fileSystem = FileSystem.get(jobConf);
            final FileSplit fileSplit = (FileSplit) inputSplit;
            final Path indexPath = fileSplit.getPath();
            m_indexStream = fileSystem.open(indexPath);

            final double xmin = jobConf.getDouble(Const.XMIN, -180.0);
            final double ymin = jobConf.getDouble(Const.YMIN, -90.0);
            final double xmax = jobConf.getDouble(Const.XMAX, 180.0);
            final double ymax = jobConf.getDouble(Const.YMAX, 90.0);

            m_logger.info(String.format("0,%.6f,%.6f 1,%.6f,%.6f", xmin, ymin, xmax, ymax));

            final QuadTree quadTree = new QuadTree(m_indexStream);
            m_iterator = quadTree.search(m_indexStream, new Extent(xmin, ymin, xmax, ymax));

            final Path dataPath = new Path(indexPath.toUri().getPath().replace(replaceIndex, replaceData));
            m_dataStream = fileSystem.open(dataPath);
        }
        else
        {
            throw new IOException("Input split is not an instance of FileSplit");
        }
    }

    @Override
    public LongWritable createKey()
    {
        return new LongWritable();
    }

    @Override
    public Text createValue()
    {
        return new Text();
    }

    @Override
    public long getPos() throws IOException
    {
        return m_indexStream.getPos();
    }

    @Override
    public float getProgress() throws IOException
    {
        return m_hasNext ? 0.0F : 1.0F;
    }

    @Override
    public void close() throws IOException
    {
        if (m_dataStream != null)
        {
            m_dataStream.close();
            m_dataStream = null;
        }
        if (m_indexStream != null)
        {
            m_indexStream.close();
            m_indexStream = null;
        }
    }

    @Override
    public boolean next(
            final LongWritable key,
            final Text value) throws IOException
    {
        m_hasNext = m_iterator.hasNext();
        if (m_hasNext)
        {
            final PointData pointData = m_iterator.next();
            key.set(pointData.address);
            m_dataStream.seek(pointData.address);
            m_byteArrayOutputStream.reset();
            while (m_dataStream.available() > 0)
            {
                final int b = m_dataStream.read();
                if (b == '\n')
                {
                    break;
                }
                else
                {
                    m_byteArrayOutputStream.write(b);
                }
            }
            value.set(m_byteArrayOutputStream.toByteArray());
        }
        return m_hasNext;
    }

}
