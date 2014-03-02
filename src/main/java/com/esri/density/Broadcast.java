package com.esri.density;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class Broadcast
        implements Writable, Comparable<Broadcast>
{
    public long zulu;
    public double xMeters;
    public double yMeters;
    public int draughtInDecimeters;
    public int voyageId;
    public String mmsi;

    public Broadcast()
    {
    }

    public Broadcast(
            final long zulu,
            final double xMeters,
            final double yMeters,
            final int draughtInDecimeters,
            final int voyageId,
            final String mmsi
    )
    {
        this.zulu = zulu;
        this.xMeters = xMeters;
        this.yMeters = yMeters;
        this.draughtInDecimeters = draughtInDecimeters;
        this.voyageId = voyageId;
        this.mmsi = mmsi;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(zulu);
        dataOutput.writeDouble(xMeters);
        dataOutput.writeDouble(yMeters);
        dataOutput.writeInt(draughtInDecimeters);
        dataOutput.writeInt(voyageId);
        dataOutput.writeUTF(mmsi);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        zulu = dataInput.readLong();
        xMeters = dataInput.readDouble();
        yMeters = dataInput.readDouble();
        draughtInDecimeters = dataInput.readInt();
        voyageId = dataInput.readInt();
        mmsi = dataInput.readUTF();
    }

    @Override
    public int compareTo(final Broadcast that)
    {
        if (this.zulu < that.zulu)
        {
            return -1;
        }
        if (this.zulu > that.zulu)
        {
            return 1;
        }
        return 0;
    }

    public void append(final StringBuilder stringBuilder)
    {
        stringBuilder.append(
                String.format("%d,%.1f,%.1f,%d,%d,%s",
                        zulu,
                        xMeters,
                        yMeters,
                        draughtInDecimeters,
                        voyageId,
                        mmsi));
    }

    public Broadcast duplicate()
    {
        return new Broadcast(zulu, xMeters, yMeters, draughtInDecimeters, voyageId, mmsi);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof Broadcast))
        {
            return false;
        }

        final Broadcast broadcast = (Broadcast) o;

        if (zulu != broadcast.zulu)
        {
            return false;
        }
        if (xMeters != broadcast.xMeters)
        {
            return false;
        }
        if (yMeters != broadcast.yMeters)
        {
            return false;
        }
        if (draughtInDecimeters != broadcast.draughtInDecimeters)
        {
            return false;
        }
        if (voyageId != broadcast.voyageId)
        {
            return false;
        }
        if (!mmsi.equals(broadcast.mmsi))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (zulu ^ (zulu >>> 32));
        long temp = Double.doubleToLongBits(xMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(yMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + draughtInDecimeters;
        result = 31 * result + voyageId;
        result = 31 * result + mmsi.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Broadcast{");
        sb.append("zulu=").append(zulu);
        sb.append(", xMeters=").append(xMeters);
        sb.append(", yMeters=").append(yMeters);
        sb.append(", draughtInDecimeters=").append(draughtInDecimeters);
        sb.append(", voyageId=").append(voyageId);
        sb.append(", mmsi='").append(mmsi).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
