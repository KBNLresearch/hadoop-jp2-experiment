package eu.scape_project.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ConversionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private JobConf jobConf;
    private String logdir;

    @Override
    public void configure(JobConf jobConf) {
        this.jobConf = jobConf;
        logdir = jobConf.get("logdir");

    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        if(!key.toString().equals("TOOLLOG")) {
            output.collect(key, new Text(values.next().toString()));
        } else {
            FileSystem fs = FileSystem.get(jobConf);
            Path path = new Path(logdir + "/TOOLLOGS.txt");
            FSDataOutputStream out = fs.create(path);

            while(values.hasNext()) {
                out.write(values.next().toString().getBytes());
            }
            out.close();
        }
    }
}
