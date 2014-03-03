AISTools
========

Set of temporal/spatial MapReduce jobs to work with AIS data.

Once the [AIS](http://www.marinecadastre.gov/AIS/default.aspx) data has been imported into HDFS using the [AISImport](https://github.com/mraad/AISImport) tool and placed in a temporal order using the file system path format `/ais/YYYY/MM/DD/HH/UUID`, the tools in this project will spatially index each UUID leaf file, in such that a temporal and spatial search can be performed on this data.  The spatial index location is by convention placed in `/ais-index/YYYY/MM/DD/HH/UUID`.

To take advantage of this dual data placement, and knowing how this AIS point data is placed and used, a non-splittable input file format is defined in this implementation to enable a temporal and spatial search [^1]. Note that the temporal search is first using the file system defined path convention, then comes the spatial search on each leaf file.   

### Maven Dependencies

* [WebMercator](https://github.com/mraad/WebMercator)
* [Shapefile](https://github.com/mraad/Shapefile)
* [FSSpatialIndex](https://github.com/mraad/FSSpatialIndex)

### Build and Package
```
$ mvn clean package
```

### Create Spatial Index
```
$ hadoop jar target/AISTools-1.0-SNAPSHOT-job.jar index /ais/2009
```
This job will iterate over the leaf files found under the specified folder and will create the equivalent index files in the `/ais-index` folder.

### Temporal Spatial Search
```
$ hadoop jar target/AISTools-1.0-SNAPSHOT-job.jar -conf conf.xml search /ais-index/2009/01/*/10/* output
```
This job searches for all AIS data in the month of January at 10AM and within an extent defined in the `conf.xml` file

```
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>com.esri.xmin.meters</name>
        <value>-8923666.0</value>
    </property>
    <property>
        <name>com.esri.ymin.meters</name>
        <value>2967457.0</value>
    </property>
    <property>
        <name>com.esri.xmax.meters</name>
        <value>-8919402.0</value>
    </property>
    <property>
        <name>com.esri.ymax.meters</name>
        <value>2970108.0</value>
    </property>
</configuration>
```
The result can exported and imported into ArcMap for visualization:

```
$ hadoop fs -cat output/part* | awk -f search.awk > /mnt/hgfs/Share/search.csv
```

![SearchResult](https://dl.dropboxusercontent.com/u/2193160/AISToolsSearch.png)

### Temporal Spatial Join Search

This job searches for all AIS data in the month of January at 10AM and within the extent of a set of hex polygons - it perform a point-in-polygon analysis and sums per hex the unique number of voyages.  The result is exported and imported into ArcMap for visualization.

Put the hex shape file into HDFS:

```
$ hadoop fs -put data/hex.shp hex.shp
```

Perform the temporal spatial join:

```
hadoop jar target/AISTools-1.0-SNAPSHOT-job.jar density-index hex.shp /ais-index/2009/01/*/10/* output
```

Export the result:

```
hadoop fs -cat output/part* | awk -f density.awk > density.csv
```

![DensityResult](https://dl.dropboxusercontent.com/u/2193160/AISToolsDensity.png)

[^1]: There is a way to make the quad tree splittable - "I have discovered a truly marvelous demonstration of this proposition that this margin is too narrow to contain"
