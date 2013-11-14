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
        if(args.length > 2) {
            conf.set("tmpdir", args[2]);
        }

        conf.set("logdir", args[1]);
        conf.set("outdir", args[1]);
        conf.setInt("mapred.line.input.format.linespermap", 1);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(ConversionMapper.class);
        conf.setReducerClass(ConversionReducer.class);
        conf.setInputFormat(NLineInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);



/*        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inFile = new Path(args[0]);

        FSDataInputStream zipfile = fs.open(inFile);
        ZipInputStream zis = new ZipInputStream(zipfile);

        ZipEntry ze = zis.getNextEntry();

        while(ze!=null){
            String fileName = ze.getName();
            FSDataOutputStream out = fs.create(new Path(fileName + "-char.txt"));
            System.out.println("characterizing: " + fileName);
            Executor exec = new DefaultExecutor();
            CommandLine cl = new CommandLine("file");
            cl.addArgument("-");
            exec.setStreamHandler(new PumpStreamHandler(out, null, zis));
            exec.execute(cl);
            ze = zis.getNextEntry();
            out.close();
        }
        zis.close();       */


    }
}
