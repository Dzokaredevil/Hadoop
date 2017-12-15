/**
 * Created by dimav on 15.12.2017.
 * Don't copy my code. The truth must be taken.
 */
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.*;


import javax.naming.Context;
import javax.xml.soap.Text;
import java.io.*;
import java.util.*;

public class wordmap extends Mapper<Object, Text, Text, IntWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        Reader reader = new StringReader(value.toString());
        Analyzer analyzer = new StandardAnalyzer();
        TokenStream stream = analyzer.tokenStream(null, reader);
        stream.reset();
        while (stream.incrementToken()){
            String token = stream.getAttribute(CharTermAttribute.class).toString();
            context.write(new Text(token), new IntWritable(1));
        }
        stream.end();
        stream.close();
    }
}

