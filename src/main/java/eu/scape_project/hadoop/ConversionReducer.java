package eu.scape_project.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ConversionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        if(!key.toString().equals("TOOLLOG")) {
            output.collect(key, new Text(values.next().toString()));
        } else {
            StringBuffer bff = new StringBuffer();
            while(values.hasNext()) {
                bff.append(values.next().toString());
            }
            output.collect(new Text("TOOLLOGS"), new Text(bff.toString()));
        }
    }
}
