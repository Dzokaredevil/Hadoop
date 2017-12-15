import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.naming.Context;
import javax.xml.soap.Text;
import java.io.IOException;

public class SpectrumMapper extends Mapper<Text, RawSpectrum, Text, RawSpectrum>
{
	private int counter = 0;
	public void map(Text key, RawSpectrum value, Context context) throws IOException, InterruptedException {
		// ограничений спектров нет для экономии пространства
		/*
		if (counter < 5) {
			++counter;
        	context.write(key, value);
		}
		*/
		context.write(key, value);
	}
}
