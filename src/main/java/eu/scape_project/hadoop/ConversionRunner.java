package eu.scape_project.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class ConversionRunner {

    public static void main(String[] args) throws IOException {

        JobConf conf = new JobConf(ConversionRunner.class);
        conf.setJobName("tif2jp2");
        if(args.length > 3) {
            conf.set("tmpdir", args[3]);
        }

        // Set the dir for logfiles to match the output directory
        conf.set("logdir", args[2]);
        conf.set("outdir", args[2]);

        // Configure mapreduce to process one file per mapper job
        conf.setInt("mapred.line.input.format.linespermap", 1);

        // Output logs as text
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(ConversionMapper.class);
        conf.setReducerClass(ConversionReducer.class);

        // Use the input keyfile with file entries (refering to separate hdfs TIFF locations)
        conf.setInputFormat(NLineInputFormat.class);

        // Output logs as text file.
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        JobClient.runJob(conf);

    }
}
