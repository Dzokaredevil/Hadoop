import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.naming.Context;
import javax.xml.soap.Text;
import java.io.*;
import java.util.*;

public class SpectrumMapper
extends Mapper<Text, RawSpectrum, Text, RawSpectrum>
{

    public void map(Text key, RawSpectrum value, Context context)
	throws IOException, InterruptedException
	{
//		String[] fields = key.toString().split(",");
//		if (fields.length == 2) {
//			String date = fields[0];
//			String station = fields[1];
//			context.write(
//		}
		// limit no. of spectra to save space
		if (counter < 5) {
			++counter;
        	context.write(key, value);
		}
    }

	private int counter = 0;

}
