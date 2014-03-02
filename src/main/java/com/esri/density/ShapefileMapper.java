package com.esri.density;

import com.esri.Const;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.QuadTree;
import com.esri.shp.ShpHeader;
import com.esri.shp.ShpReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public abstract class ShapefileMapper extends AbstractMapper<IntWritable, IntWritable>
{
    protected IntWritable m_origID = new IntWritable();
    protected Point m_point = new Point();
    protected Envelope2D m_envelope2D = new Envelope2D();
    protected List<Polygon> m_list = new ArrayList<Polygon>();
    protected Set<Integer> m_set = new HashSet<Integer>();
    protected Envelope2D m_extent;
    protected QuadTree m_quadTree;
    protected QuadTree.QuadTreeIterator m_quadTreeIterator;
    protected OperatorContains m_operatorContains;
    private double m_tolerance;

    @Override
    public void configure(final JobConf jobConf)
    {
        super.configure(jobConf);
        m_tolerance = jobConf.getFloat("com.esri.quadtree.tolerance", 0.000001F);
        try
        {
            m_operatorContains = OperatorContains.local();
            final InputStream inputStream = getInputStream();
            try
            {
                int element = 0;
                final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
                final ShpHeader shpHeader = shpReader.getHeader();
                m_extent = new Envelope2D(shpHeader.xmin, shpHeader.ymin, shpHeader.xmax, shpHeader.ymax);
                final int depth = jobConf.getInt("com.esri.quadtree.depth", 8);
                m_quadTree = new QuadTree(m_extent, depth);
                final Envelope2D boundingBox = new Envelope2D();
                while (shpReader.hasMore())
                {
                    final Polygon polygon = shpReader.readPolygon();
                    polygon.queryEnvelope2D(boundingBox);
                    m_quadTree.insert(element++, boundingBox);
                    m_list.add(polygon);
                }
                m_quadTreeIterator = m_quadTree.getIterator();
                m_logger.info("Read {} features from DistributedCache.", element);
            }
            finally
            {
                inputStream.close();
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public InputStream getInputStream() throws IOException
    {
        final File file = new File("./shp"); // read from DistributedCache
        return file.toURI().toURL().openStream();
    }

    @Override
    public void map(
            final LongWritable longWritable,
            final Text text,
            final OutputCollector<IntWritable, IntWritable> collector,
            final Reporter reporter) throws IOException
    {
        try
        {
            tokenize(text);
            final double x = m_broadcast.xMeters;
            final double y = m_broadcast.yMeters;
            if (m_extent.contains(x, y))
            {
                m_point.setXY(x, y);
                m_envelope2D.setCoords(x, y, x, y);
                m_quadTreeIterator.resetIterator(m_envelope2D, m_tolerance);
                int elementIndex = m_quadTreeIterator.next();
                while (elementIndex > -1)
                {
                    final int listIndex = m_quadTree.getElement(elementIndex);
                    final Polygon polygon = m_list.get(listIndex);
                    if (m_set.contains(listIndex) == false)
                    {
                        m_set.add(listIndex);
                        m_operatorContains.accelerateGeometry(polygon, null, Geometry.GeometryAccelerationDegree.enumHot);
                    }
                    if (m_operatorContains.execute(polygon, m_point, null, null))
                    {
                        m_origID.set(listIndex);
                        collect(collector, reporter);
                        break;
                    }
                    elementIndex = m_quadTreeIterator.next();
                }
            }
        }
        catch (Throwable t)
        {
            reporter.getCounter(Const.MAPPER, Const.BAD_RECORDS).increment(1L);
        }
    }

    protected abstract void collect(
            final OutputCollector<IntWritable, IntWritable> collector,
            final Reporter reporter) throws IOException;
}
