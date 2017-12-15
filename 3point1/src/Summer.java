import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.naming.Context;
import javax.xml.soap.Text;
import java.io.IOException;

public class Summer extends Reducer<Text, RawSpectrum, Text, RawSpectrum> {
    public void reduce(Text key, Iterable<RawSpectrum> values, Context context)
	throws IOException, InterruptedException {
        for (RawSpectrum val : values) {
            context.write(key, val);
        }
    }
}
