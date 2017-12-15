import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.xml.soap.Text;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HadoopDriver extends Configured implements Tool {
    private static Logger LOGGER = Logger.getLogger(HadoopDriver.class.getName());
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        System.exit(ret);
    }
    public int run(String[] args) throws Exception {
        LOGGER.setLevel(Level.INFO);
        if (args.length < 2) {
			ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }

        //нигде не используется
		boolean countTotal = false;
		if (args.length >= 3 && "--total".equals(args[3])) {
			countTotal = true;
		}

		Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopDriver.class);
		job.setJobName("Spectrum");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(SpectrumMapper.class);
        //job.setReducerClass(Summer.class);

        //стандартно
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RawSpectrum.class);

        //наследник InputFormat
		job.setInputFormatClass(SpectrumInputFormat.class);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
