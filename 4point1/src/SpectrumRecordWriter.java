import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import ucar.ma2.*;
import ucar.nc2.*;
import ucar.nc2.util.*;

public class SpectrumRecordWriter extends RecordWriter<Text,RawSpectrum>
{
	private String filename;
	private NetcdfFileWriter writer;
	private Dimension time;
	private Dimension frequency;

	private ArrayFloat iData;
	private ArrayFloat jData;
	private ArrayFloat kData;
	private ArrayFloat wData;
	private ArrayFloat dData;

	private Variable i;
	private Variable j;
	private Variable k;
	private Variable w;
	private Variable d;
	private int timeIndex = 0;

	public SpectrumRecordWriter(String filename) throws IOException
	{
		this.filename = filename;

		writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "/mnt/root/" + filename.toString());

		time = writer.addUnlimitedDimension("time");
		frequency = writer.addDimension(null, "frequency", 47);

		i = writer.addVariable(null, "i", DataType.FLOAT, "time frequency");
		j = writer.addVariable(null, "j", DataType.FLOAT, "time frequency");
		k = writer.addVariable(null, "k", DataType.FLOAT, "time frequency");
		w = writer.addVariable(null, "w", DataType.FLOAT, "time frequency");
		d = writer.addVariable(null, "d", DataType.FLOAT, "time frequency");

		writer.create();

		iData = new ArrayFloat.D2(1, frequency.getLength());
		jData = new ArrayFloat.D2(1, frequency.getLength());
		kData = new ArrayFloat.D2(1, frequency.getLength());
		wData = new ArrayFloat.D2(1, frequency.getLength());
		dData = new ArrayFloat.D2(1, frequency.getLength());

	}

	@Override
	public void
	write(Text key, RawSpectrum value) throws IOException
	{
		float[] iArray = value.getI();
		float[] jArray = value.getJ();
		float[] kArray = value.getK();
		float[] wArray = value.getW();
		float[] dArray = value.getD();
		for (int it = 0; it < value.getI().length; ++it)
		{

			iData.set(iData.getIndex().set(0, it), iArray[it]);
			jData.set(jData.getIndex().set(0, it), jArray[it]);
			kData.set(kData.getIndex().set(0, it), kArray[it]);
			wData.set(wData.getIndex().set(0, it), wArray[it]);
			dData.set(dData.getIndex().set(0, it), dArray[it]);

		}
		int[] origin = new int[]{0, 0};
		origin[0] = timeIndex;

		try
		{
			writer.write(i, origin, iData);
			writer.write(j, origin, jData);
			writer.write(k, origin, kData);
			writer.write(w, origin, wData);
			writer.write(d, origin, dData);

		}
		catch (InvalidRangeException e)
		{
			e.printStackTrace();
		}
		timeIndex++;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException
	{
		writer.close();
	}
}
