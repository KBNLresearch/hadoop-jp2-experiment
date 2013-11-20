package eu.scape_project.hadoop;

import eu.scape_project.hadoop.util.CliCommand;
import eu.scape_project.hadoop.util.LocalFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class ConversionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static final String sep = System.getProperty("line.separator");
    private String tempdir;
    private String outdir;

    @Override
    public void configure(JobConf job) {
        tempdir = job.get("tmpdir");
        outdir = job.get("outdir");
    }


    @Override
    public void map(LongWritable ignored, Text filepath,
                    OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


        FileSystem fs = FileSystem.get(new Configuration());
        String probatronJAR = ConversionRunner.class.getResource("/external-tools/probatron.jar").getFile();
        String probatronSchema = ConversionRunner.class.getResource("/kbMaster.sch").getFile();

        LocalFile tif = new LocalFile(tempdir + "/" + filepath.toString().replaceAll(".+\\/", ""), filepath.toString(), fs);
        LocalFile jp2 = new LocalFile(tif.getAbsolutePath() + ".jp2");
        LocalFile outtif = new LocalFile(tif.getAbsolutePath() + "out.tif");
        LocalFile profile = new LocalFile(tif.getAbsolutePath() + ".profile.xml");

        StringBuffer report = new StringBuffer(sep);
        StringBuffer toolLogs = new StringBuffer(sep + sep + "TOOL LOGS FOR " + filepath + ":" + sep + "==================" + sep);

        try {

            CliCommand opj_compress = new CliCommand(tif, jp2);
            opj_compress.runCommand("opj_compress", "-n", "6", "-t", "512,512", "-i", "#infile#", "-o", "#outfile#");
            report.append(opj_compress.getElapsedTime() + ";");
            report.append("SUCCESS;");

            toolLogs.append("opj_compress OUT:" + sep + "---" + sep + opj_compress.getStdOut() + sep + sep);
            toolLogs.append("opj_compress ERR:" + sep + "---" + sep + opj_compress.getStdErr() + sep + sep);

            fs.copyFromLocalFile(new Path(jp2.getAbsolutePath()), new Path(outdir + "/" + jp2.getName()));

            CliCommand jpylyzer = new CliCommand(jp2);
            jpylyzer.runCommand("jpylyzer", "#infile#");
            report.append(jpylyzer.getElapsedTime() + ";");

            toolLogs.append("jpylyzer OUT:" + sep + "---" + sep + jpylyzer.getStdOut() + sep + sep);
            toolLogs.append("jpylyzer ERR:" + sep + "---" + sep + jpylyzer.getStdErr() + sep + sep);

/*            FileWriter w = new FileWriter(new File(profile.getAbsolutePath()));
            w.write(jpylyzer.getStdOut());
            w.close();           */

            if(jpylyzer.getStdOut().contains("<isValidJP2>True</isValidJP2>")) {
                report.append("SUCCESS;");
            }  else {
                report.append("FAILURE;");
            }

            CliCommand probatron = new CliCommand(profile);
            try {
                probatron.runCommand("java", "-jar", probatronJAR, "#infile#", probatronSchema);
            } catch (IOException e) {

            }
            report.append(probatron.getElapsedTime() + ";");

            if(probatron.getStdOut().contains("failed-assert")) {
                report.append("FAILURE;");
            } else {
                report.append("SUCCESS;");
            }

            toolLogs.append("probatron OUT:" + sep + "---" + sep + probatron.getStdOut() + sep + sep);
            toolLogs.append("probatron ERR:" + sep + "---" + sep + probatron.getStdErr() + sep + sep);


            CliCommand kdu_expand = new CliCommand(jp2, outtif);
            kdu_expand.runCommand("kdu_expand", "-i", "#infile#", "-o", "#outfile#");
            report.append(kdu_expand.getElapsedTime() + ";");
            report.append("SUCCESS;");

            toolLogs.append("kdu_expand OUT:" + sep + "---" + sep + kdu_expand.getStdOut() + sep + sep);
            toolLogs.append("kdu_expand ERR:" + sep + "---" + sep + kdu_expand.getStdErr() + sep + sep);


            CliCommand gm = new CliCommand(outtif, tif);
            gm.runCommand("gm", "compare", "-metric", "mse", "#infile#", "#outfile#");
            report.append(gm.getElapsedTime() + ";");
            if(gm.getStdOut().contains("Total: 0.0000000000")) {
                report.append("SUCCESS;");
            } else {
                report.append("FAILURE;");
            }
            toolLogs.append("gm compare OUT:" + sep + "---" + sep + gm.getStdOut() + sep + sep);
            toolLogs.append("gm compare ERR:" + sep + "---" + sep + gm.getStdErr() + sep + sep);

        } catch(IOException e) {
            report.append("IOEXCEPTION: " + e.getMessage());
        } finally {

            tif.delete();
            jp2.delete();
            outtif.delete();
        }

        output.collect(new Text("TOOLLOG"), new Text(toolLogs.toString()));
        output.collect(filepath, new Text(report.toString()));
    }
}
