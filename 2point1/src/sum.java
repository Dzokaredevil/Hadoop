/**
 * Created by dimav on 15.12.2017.
 * Don't copy my code. The truth must be taken.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.naming.Context;
import javax.xml.soap.Text;
import java.io.*;
import java.util.*;

public class sum extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int summer = 0;
        for (IntWritable val : values){
            summer += val.get();
        }
        context.write(key, new IntWritable(summer));
    }
}