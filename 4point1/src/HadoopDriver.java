import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.xml.soap.Text;
import java.nio.file.Path;
import java.io.*;
import java.util.*;

public class HadoopDriver extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        System.exit(ret);
    }
    public int run(String[] args) throws Exception
    {
        if (args.length < 2)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }
        boolean countTotal = false;
        if (args.length >= 3 && "--total".equals(args[3])) {
            countTotal = true;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopDriver.class);
        job.setJobName("NetCDF");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(SpectrumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RawSpectrum.class);
        job.setInputFormatClass(SpectrumInputFormat.class);
        job.setOutputFormatClass(SpectrumOutputFormat.class);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
