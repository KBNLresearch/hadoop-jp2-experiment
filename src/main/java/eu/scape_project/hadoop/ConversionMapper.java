package eu.scape_project.hadoop;

import eu.scape_project.hadoop.util.CliCommand;
import eu.scape_project.hadoop.util.LocalFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import eu.scape_project.hadoop.util.Settings;
import java.io.IOException;


public class ConversionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static final String sep = System.getProperty("line.separator");
    @Override
    public void configure(JobConf job) {
        Settings.tempdir = job.get("tmpdir");
        Settings.logdir = job.get("logdir");
        Settings.outdir = job.get("outdir");
    }


    @Override
    public void map(LongWritable ignored, Text filepath,
                    OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


        FileSystem fs = FileSystem.get(new Configuration());
        LocalFile tif = new LocalFile(Settings.tempdir + "/" + filepath.toString().replaceAll(".+\\/", ""), filepath.toString(), fs);
        LocalFile jp2 = new LocalFile(tif.getPath() + ".jp2");
        LocalFile tga = new LocalFile(tif.getPath() + ".tga");
        StringBuffer report = new StringBuffer(sep);
        StringBuffer toolLogs = new StringBuffer(sep + sep + "TOOL LOGS FOR " + filepath + ":" + sep + "==================" + sep);

        try {

            CliCommand opj_compress = new CliCommand(tif, jp2);
            opj_compress.runCommand("opj_compress", "-n", "6", "-t", "512,512", "-i", "#infile#", "-o", "#outfile#");
            report.append(opj_compress.getElapsedTime() + ";");
            report.append("SUCCESS;");

            toolLogs.append("opj_compress OUT:" + sep + "---" + sep + opj_compress.getStdOut() + sep + sep);
            toolLogs.append("opj_compress ERR:" + sep + "---" + sep + opj_compress.getStdErr() + sep + sep);

            fs.copyFromLocalFile(new Path(jp2.getPath()), new Path(Settings.outdir + "/" + jp2.getName()));

            CliCommand jpylyzer = new CliCommand(jp2);
            jpylyzer.runCommand("jpylyzer", "#infile#");
            report.append(jpylyzer.getElapsedTime() + ";");

            toolLogs.append("jpylyzer OUT:" + sep + "---" + sep + jpylyzer.getStdOut() + sep + sep);
            toolLogs.append("jpylyzer ERR:" + sep + "---" + sep + jpylyzer.getStdErr() + sep + sep);

            if(jpylyzer.getStdOut().contains("<isValidJP2>True</isValidJP2>")) {
                report.append("SUCCESS;");
            }  else {
                report.append("FAILURE;");
            }

            CliCommand opj_decompress = new CliCommand(jp2, tga);
            opj_decompress.runCommand("opj_decompress", "-i", "#infile#", "-o", "#outfile#");
            report.append(opj_decompress.getElapsedTime() + ";");
            report.append("SUCCESS;");

            toolLogs.append("opj_decompress OUT:" + sep + "---" + sep + opj_decompress.getStdOut() + sep + sep);
            toolLogs.append("opj_decompress ERR:" + sep + "---" + sep + opj_decompress.getStdErr() + sep + sep);


            CliCommand gm = new CliCommand(tga, tif);
            gm.runCommand("gm", "compare", "-metric", "mse", "#infile#", "#outfile#");
            report.append(gm.getElapsedTime() + ";");
            if(gm.getStdOut().contains("Total: 0.0000000000")) {
                report.append("SUCCESS;");
            } else {
                report.append("FAILURE;");
            }
            toolLogs.append("gm compare OUT:" + sep + "---" + sep + opj_decompress.getStdOut() + sep + sep);
            toolLogs.append("gm compare ERR:" + sep + "---" + sep + opj_decompress.getStdErr() + sep + sep);

        } catch(IOException e) {
            report.append("IOEXCEPTION: " + e.getMessage());
        } finally {

            tif.delete();
            jp2.delete();
            tga.delete();
        }

        output.collect(new Text("TOOLLOG"), new Text(toolLogs.toString()));
        output.collect(filepath, new Text(report.toString()));
    }
}
