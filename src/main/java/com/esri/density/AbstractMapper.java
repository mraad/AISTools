package com.esri.density;

import com.esri.Const;
import com.esri.FastTok;
import com.esri.WebMercator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 */
public abstract class AbstractMapper<K, V>
        extends MapReduceBase
        implements Mapper<LongWritable, Text, K, V>
{
    protected final Logger m_logger = LoggerFactory.getLogger(getClass());
    protected final SimpleDateFormat m_simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    protected final FastTok m_fastTok = new FastTok();

    protected int m_indexVoyageId;
    protected int m_indexLon;
    protected int m_indexLat;
    protected int m_indexZulu;
    protected int m_indexMMSI;
    protected char m_fieldSep;

    protected IntWritable m_voyageId = new IntWritable();
    protected Broadcast m_broadcast = new Broadcast();

    @Override
    public void configure(final JobConf job)
    {
        m_simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        m_indexLon = job.getInt("com.esri.index.lon", Const.INDEX_LON);
        m_indexLat = job.getInt("com.esri.index.lat", Const.INDEX_LAT);
        m_indexZulu = job.getInt("com.esri.index.zulu", Const.INDEX_ZULU);
        m_indexVoyageId = job.getInt("com.esri.index.voyageid", Const.INDEX_VOYAGE_ID);
        m_indexMMSI = job.getInt("com.esri.index.mmsi", Const.INDEX_MMSI);
        m_fieldSep = job.get("com.esri.field.separator", "\t").charAt(0);
    }

    protected void tokenize(final Text text) throws ParseException
    {
        m_fastTok.tokenize(text.toString(), m_fieldSep);

        final String[] tokens = m_fastTok.tokens;

        final int voyageId = Integer.parseInt(tokens[m_indexVoyageId]);
        m_voyageId.set(voyageId);

        m_broadcast.voyageId = voyageId;
        m_broadcast.zulu = toZulu(tokens[m_indexZulu]);
        m_broadcast.mmsi = tokens[m_indexMMSI];
        m_broadcast.xMeters = WebMercator.longitudeToX(Double.parseDouble(tokens[m_indexLon]));
        m_broadcast.yMeters = WebMercator.latitudeToY(Double.parseDouble(tokens[m_indexLat]));
    }

    protected long toZulu(final String text) throws ParseException
    {
        return m_simpleDateFormat.parse(text).getTime();
    }
}
