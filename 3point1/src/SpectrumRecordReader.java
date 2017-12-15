import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.xml.soap.Text;
import java.io.*;
import java.util.*;
import java.util.logging.Formatter;
import java.util.regex.*;
import java.util.zip.*;
import java.util.logging.*;


public class SpectrumRecordReader extends RecordReader<Text, RawSpectrum>
{
    private static final Logger LOGGER = Logger.getLogger(SpectrumInputFormat.class.getName());
    // пример: 2017 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
    private static final Pattern LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");
    private long totalSize = 0;
    private long currentSize = 0;
    private Text date = null;
    private RawSpectrum rawSpectrum = null;
    private HashMap<String, HashMap<Text, float[]>> hashMap;
    private Path[] paths;
    private Iterator<Map.Entry<Text, float[]>> it;
    @Override
    public Text getCurrentKey()
    {
        return date;
    }
    @Override
    public RawSpectrum getCurrentValue()
    {
        return rawSpectrum;
    }
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        CombineFileSplit split = (CombineFileSplit) inputSplit;
        paths = split.getPaths();
        Formatter formatter = new SimpleFormatter();
        Handler handler = new FileHandler("/home/rstm2013/logging/3/log" + paths[0].getName());
        handler.setFormatter(formatter);
        LOGGER.addHandler(handler);
        hashMap = new HashMap<String, HashMap<Text, float[]>>();
        hashMap.put("i", new HashMap<Text, float[]>());
        hashMap.put("j", new HashMap<Text, float[]>());
        hashMap.put("k", new HashMap<Text, float[]>());
        hashMap.put("w", new HashMap<Text, float[]>());
        hashMap.put("d", new HashMap<Text, float[]>());

        for (int i = 0; i < paths.length; ++i) {
            FileSystem fileSystem = paths[i].getFileSystem(context.getConfiguration());
            FSDataInputStream inputStream = fileSystem.open(paths[i]);
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            LineReader reader = new LineReader(gzipInputStream, context.getConfiguration());
            Matcher fileNameMatcher = RawSpectrum.FILENAME_PATTERN.matcher(paths[i].getName());
            if (fileNameMatcher.matches()) {
                Text line = new Text();
                Matcher lineMatcher;
                String variable = fileNameMatcher.group(2);
                Text tmpDate;
                String floatsString;
                while (reader.readLine(line) != 0) {
                    lineMatcher = LINE_PATTERN.matcher(line.toString());
                    if (lineMatcher.matches()) {
                        tmpDate = new Text(lineMatcher.group(1));
                        floatsString = lineMatcher.group(2);
                        ArrayList<Float> floatsArrayList = new ArrayList<Float>();
                        Scanner scanner = new Scanner(floatsString);

                        while (scanner.hasNextFloat()) {
                            floatsArrayList.add(scanner.nextFloat());
                        }

                        float[] floats = new float[floatsArrayList.size()];
                        for (int j = 0; j < floats.length; ++j) {
                            floats[j] = floatsArrayList.get(j);
                        }
                        floatsArrayList.clear();
                        hashMap.get(variable).put(tmpDate, floats);
                    }
                }
            }
            reader.close();
            gzipInputStream.close();
            fileSystem.close();
        }
        String min = "i";
        for (String tmp : hashMap.keySet()) {
            if (hashMap.get(tmp).size() < hashMap.get(min).size()) {
                min = tmp;
            }
        }
        it = hashMap.get(min).entrySet().iterator();
        totalSize = hashMap.get(min).size();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public float getProgress() {
        if (totalSize == 0) return 1f;
        else return (float) currentSize / (float) totalSize;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        boolean found = false;
        Map.Entry<Text, float[]> entry;
        while (!found && it.hasNext()) {
            entry = (Map.Entry<Text, float[]>) it.next();
            currentSize += 1;
            boolean contains = true;
            if (!hashMap.get("i").containsKey(entry.getKey())) contains = false;
            if (!hashMap.get("j").containsKey(entry.getKey())) contains = false;
            if (!hashMap.get("k").containsKey(entry.getKey())) contains = false;
            if (!hashMap.get("w").containsKey(entry.getKey())) contains = false;
            if (!hashMap.get("d").containsKey(entry.getKey())) contains = false;
            if (contains) {
                found = true;
                date = new Text(entry.getKey());
                rawSpectrum = new RawSpectrum();
                rawSpectrum.setFieldRstm("i", hashMap.get("i").get(entry.getKey()));
                rawSpectrum.setFieldRstm("j", hashMap.get("j").get(entry.getKey()));
                rawSpectrum.setFieldRstm("k", hashMap.get("k").get(entry.getKey()));
                rawSpectrum.setFieldRstm("w", hashMap.get("w").get(entry.getKey()));
                rawSpectrum.setFieldRstm("d", hashMap.get("d").get(entry.getKey()));
            }
        }
        return found;
    }
}