import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.xml.soap.Text;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpectrumInputFormat
extends InputFormat<Text,RawSpectrum>
{
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
	{
		HashMap<String, ArrayList<FileStatus>> deviceNumberAndPath = new HashMap();
		for (Path path : FileInputFormat.getInputPaths(context))
		{
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			for (FileStatus file : fs.listStatus(path))
			{
				String deviceNumber = file.getPath().getName().toString().substring(0, 5);
				if (!deviceNumberAndPath.containsKey(deviceNumber))
				{
					ArrayList<FileStatus> filesForDevice = new ArrayList<FileStatus>();
					filesForDevice.add(file);
					deviceNumberAndPath.put(deviceNumber, filesForDevice);
				}
				else
				{
					deviceNumberAndPath.get(deviceNumber).add(file);
				}
			}
		}
		ArrayList<InputSplit> combineFileSplits = new ArrayList<>();
		for (Map.Entry<String, ArrayList<FileStatus>> oneSplit : deviceNumberAndPath.entrySet())
		{
			int size = oneSplit.getValue().size();
			if (size == 5)
			{
				Path[] paths = new Path[5];
				long[] fileSizes = new long[5];
				FileStatus[] fileStatuses = new FileStatus[5];
				fileStatuses = oneSplit.getValue().toArray(fileStatuses);
				for (int i = 0; i < 5; ++i)
				{
					paths[i] = fileStatuses[i].getPath();
					fileSizes[i] = fileStatuses[i].getLen();
				}
				combineFileSplits.add(new CombineFileSplit(paths, fileSizes));
			}
		}
		return combineFileSplits;
	}

	@Override
	public RecordReader<Text, RawSpectrum> createRecordReader(InputSplit split, TaskAttemptContext context) throws
			IOException, InterruptedException
	{
		return new SpectrumRecordReader();
	}
}
