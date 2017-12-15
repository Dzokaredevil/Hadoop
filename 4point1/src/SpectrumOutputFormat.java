import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.xml.soap.Text;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.*;

public class SpectrumOutputFormat
extends FileOutputFormat<Text,RawSpectrum>
{
	@Override
	public RecordWriter<Text,RawSpectrum>
	getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Path outdir = FileOutputFormat.getOutputPath(context);
		outdir = new Path(outdir, "result.nc");
		String glusterPath = outdir.toUri().getPath();
		System.out.println("Output directry = " + glusterPath);
		return new SpectrumRecordWriter(glusterPath);
	}

}
