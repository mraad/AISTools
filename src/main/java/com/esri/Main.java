package com.esri;

import com.esri.hadoop.Extent;
import com.esri.hadoop.quadtree.FSQuadTreeWriter;
import com.esri.hadoop.quadtree.PointData;
import com.esri.mapred.QuadTreeFileInputFormat;
import com.esri.mapred.QuadTreeMapper;
import com.esri.shp.ShpHeader;
import com.esri.density.DensityMapper;
import com.esri.density.DensityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 */
public class Main extends Configured implements Tool
{
    final Logger m_logger = LoggerFactory.getLogger(Main.class);

    @Override
    public int run(final String[] args) throws Exception
    {
        if (args.length == 0)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        if (args.length == 2 && "index".equalsIgnoreCase(args[0]))
        {
            doIndex(args[1]);
        }
        else if (args.length == 3 && "search".equalsIgnoreCase(args[0]))
        {
            doSearch(args);
        }
        else if (args.length == 4 && "com/esri/density".equalsIgnoreCase(args[0]))
        {
            doDensity(args, false);
        }
        else if (args.length == 4 && "density-index".equalsIgnoreCase(args[0]))
        {
            doDensity(args, true);
        }
        else
        {
            System.err.println("Usage:");
            System.err.println("  hadoop jar [jarfile] index [input path] [output path]");
            System.err.println("  hadoop jar [jarfile] search [input path] [output path]");
            System.err.println("  hadoop jar [jarfile] density [shapefile] [input path] [output path]");
        }
        return 0;
    }

    private void doDensity(
            final String[] args,
            final boolean useIndex) throws IOException, URISyntaxException
    {
        final JobConf jobConf = new JobConf(getConf(), Main.class);

        if (useIndex)
        {
            jobConf.setJobName("DensityQuadTree");
            jobConf.setInputFormat(QuadTreeFileInputFormat.class);
        }
        else
        {
            jobConf.setJobName("Density");
            jobConf.setInputFormat(TextInputFormat.class);
        }

        DistributedCache.createSymlink(jobConf);
        DistributedCache.addCacheFile(new URI(args[1] + "#shp"), jobConf);

        setExtent(jobConf, args[1]);

        FileInputFormat.setInputPaths(jobConf, new Path(args[2]));

        jobConf.setMapperClass(DensityMapper.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(IntWritable.class);

        jobConf.setReducerClass(DensityReducer.class);
        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setOutputFormat(TextOutputFormat.class);

        final Path outputDir = new Path(args[3]);
        outputDir.getFileSystem(jobConf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(jobConf, outputDir);

        JobClient.runJob(jobConf);
    }

    private void setExtent(
            final JobConf jobConf,
            final String pathName) throws IOException
    {
        final FileSystem fileSystem = FileSystem.get(jobConf);
        final FSDataInputStream dataInputStream = fileSystem.open(new Path(pathName));
        try
        {
            final ShpHeader shpHeader = new ShpHeader(dataInputStream);
            jobConf.setDouble(Const.XMIN, shpHeader.xmin);
            jobConf.setDouble(Const.YMIN, shpHeader.ymin);
            jobConf.setDouble(Const.XMAX, shpHeader.xmax);
            jobConf.setDouble(Const.YMAX, shpHeader.ymax);
        }
        finally
        {
            dataInputStream.close();
        }
    }

    private void doSearch(final String[] args) throws IOException
    {
        final JobConf jobConf = new JobConf(getConf(), Main.class);
        jobConf.setJobName("Search");

        jobConf.setMapperClass(QuadTreeMapper.class);

        jobConf.setMapOutputKeyClass(NullWritable.class);
        jobConf.setMapOutputValueClass(Writable.class);

        jobConf.setNumReduceTasks(0);

        jobConf.setInputFormat(QuadTreeFileInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
        final Path outputDir = new Path(args[2]);
        outputDir.getFileSystem(jobConf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(jobConf, outputDir);

        JobClient.runJob(jobConf);
    }

    private void doIndex(final String pathName) throws IOException
    {
        final Path pathData = new Path(pathName);
        final FileSystem fileSystem = FileSystem.get(getConf());
        final FileStatus fileStatus = fileSystem.getFileStatus(pathData);
        if (fileStatus.isFile())
        {
            doIndexFile(fileSystem, pathData);
        }
        else if (fileStatus.isDirectory())
        {
            doIndexDirectory(fileSystem, pathData);
        }
    }

    private void doIndexDirectory(
            final FileSystem fileSystem,
            final Path pathData) throws IOException
    {
        final FileStatus[] fileStatuses = fileSystem.listStatus(pathData);
        for (final FileStatus fileStatus : fileStatuses)
        {
            if (fileStatus.isDirectory())
            {
                doIndexDirectory(fileSystem, fileStatus.getPath());
            }
            else
            {
                doIndexFile(fileSystem, fileStatus.getPath());
            }
        }
    }

    private void doIndexFile(
            final FileSystem fileSystem,
            final Path pathData) throws IOException
    {
        final Configuration conf = fileSystem.getConf();

        final double xmin = conf.getFloat(Const.XMIN, -180.0F);
        final double ymin = conf.getFloat(Const.YMIN, -90.0F);
        final double xmax = conf.getFloat(Const.XMAX, 180.0F);
        final double ymax = conf.getFloat(Const.YMAX, 90.0F);
        final Extent extent = new Extent(xmin, ymin, xmax, ymax);

        final int bucketSize = conf.getInt("com.esri.bucket.size", 32);
        final int indexX = conf.getInt("com.esri.index.x", 1);
        final int indexY = conf.getInt("com.esri.index.y", 2);

        final char charSep = conf.get("com.esri.field.sep", "\t").charAt(0);

        final String replaceData = conf.get(Const.PATH_DATA, "/ais");
        final String replaceIndex = conf.get(   Const.PATH_INDEX, "/ais-index");

        final String pathName = pathData.toUri().getPath();
        m_logger.info(pathName);
        final Path pathIndex = new Path(pathName.replace(replaceData, replaceIndex));
final FSDataOutputStream dataOutputStream = fileSystem.create(pathIndex, true);
final FSQuadTreeWriter quadTreeWriter = new FSQuadTreeWriter(dataOutputStream, bucketSize, extent);
try
{
    final FSDataInputStream dataInputStream = fileSystem.open(pathData);
    try
    {
        final FastTok fastTok = new FastTok();
        final LineReader lineReader = new LineReader(dataInputStream);
        final Text text = new Text();
        long pos = 0;
        int len = lineReader.readLine(text);
        while (len > 0)
        {
            fastTok.tokenize(text.toString(), charSep);
            final double x = Double.parseDouble(fastTok.tokens[indexX]);
            final double y = Double.parseDouble(fastTok.tokens[indexY]);
            quadTreeWriter.addPointData(new PointData(x, y, pos));
            pos += len;
            len = lineReader.readLine(text);
        }
    }
    finally
    {
        dataInputStream.close();
    }
}
finally
{
    quadTreeWriter.close();
}
    }

    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Main(), args));
    }
}
