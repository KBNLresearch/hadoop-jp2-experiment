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
import org.probatron.ValidationReport;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class ConversionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static final String sep = System.getProperty("line.separator");
    private String tempdir;
    private String outdir;
    private LocalFile probatronSchema;
    private LocalFile awareOpts;

    /**
     * Copy a resource file to the local fs
     * @param resourcePath
     * @return LocalFile instance containing refs to the local file
     * @throws IOException
     */
    private LocalFile makeTempFile(String resourcePath) throws IOException {
        LocalFile localFile = new LocalFile(tempdir + resourcePath);
        if(!new File(localFile.getAbsolutePath()).exists()) {

            InputStream in = ConversionRunner.class.getResourceAsStream(resourcePath);
            OutputStream out = new FileOutputStream(new File(localFile.getAbsolutePath()));
            byte[] buf = new byte[1024];
            int ln = in.read(buf);
            while(ln != -1) { out.write(buf, 0, ln); ln = in.read(buf); }
            in.close();
            out.close();
        }
		return localFile;
	}

    @Override
    public void configure(JobConf job) {
        tempdir = job.get("tmpdir");
        outdir = job.get("outdir");
        try {
			probatronSchema = makeTempFile("/kbMaster.sch");
			awareOpts = makeTempFile("/optionsKBMasterLossless.xml");

        } catch(IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }


    /**
     * Implements the actual conversion workflow
     * @param ignored
     * @param filepath hdfs file location (from a keyfile (txt))
     * @param output
     * @param reporter
     * @throws IOException
     */
    @Override
    public void map(LongWritable ignored, Text filepath,
                    OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


        FileSystem fs = FileSystem.get(new Configuration());

        // TIFF in
        LocalFile tif = new LocalFile(tempdir + "/" + filepath.toString().replaceAll(".+\\/", ""), filepath.toString(), fs);
        // JP2 out
        LocalFile jp2 = new LocalFile(tif.getAbsolutePath() + ".jp2");
        // Used for pixel check (jp2 converted back to tif using kdu_expand)
        LocalFile outtif = new LocalFile(tif.getAbsolutePath() + "out.tif");
        // KB Master probaton profile for valid .jp2 files
        LocalFile profile = new LocalFile(tif.getAbsolutePath() + ".profile.xml");

        // log report entries
        StringBuffer report = new StringBuffer(sep);
        StringBuffer toolLogs = new StringBuffer(sep + sep + "TOOL LOGS FOR " + filepath + ":" + sep + "==================" + sep);
        String currentStage = "aware_compress";

        try {
            // Compress tiff to .jp2 using aware driver (wrapped by jkn)
			CliCommand aware_compress = new CliCommand(tif, jp2);
			aware_compress.runCommand("jpwrappa.py",  "#infile#", "#outfile#", "-p", awareOpts.getAbsolutePath());
            report.append(aware_compress.getElapsedTime() + ";");
            report.append("SUCCESS;");
            toolLogs.append("jpwrappa.py OUT:" + sep + "---" + sep + aware_compress.getStdOut() + sep + sep);
            toolLogs.append("jpwrappa.py ERR:" + sep + "---" + sep + aware_compress.getStdErr() + sep + sep);

            // copy the output .jp2 to hdfs
            fs.copyFromLocalFile(new Path(jp2.getAbsolutePath()), new Path(outdir + "/" + jp2.getName()));

            currentStage = "jpylyzer";

            // Validate the .jp2 file with jpylyzer
            CliCommand jpylyzer = new CliCommand(jp2);
            jpylyzer.runCommand("jpylyzer", "#infile#");
            report.append(jpylyzer.getElapsedTime() + ";");

            toolLogs.append("jpylyzer OUT:" + sep + "---" + sep + jpylyzer.getStdOut() + sep + sep);
            toolLogs.append("jpylyzer ERR:" + sep + "---" + sep + jpylyzer.getStdErr() + sep + sep);


            FileWriter w = new FileWriter(new File(profile.getAbsolutePath()));
            w.write(jpylyzer.getStdOut());
            w.close();

            if(jpylyzer.getStdOut().contains("<isValidJP2>True</isValidJP2>")) {
                report.append("SUCCESS;");
            }  else {
                report.append("FAILURE;");
            }

            // Validate the jpylyzer profile against the KB master schema using probatron
            currentStage = "probatron";
            long probatronStart = System.nanoTime();
            org.probatron.Session ses = new org.probatron.Session();
            ses.setSchemaDoc("file:" + probatronSchema.getAbsolutePath());

            ValidationReport probatronVR = ses.doValidation("file:" + profile.getAbsolutePath());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            probatronVR.streamOut(baos);
            String probatronReport = baos.toString();
            long probatronElapsed = System.nanoTime() - probatronStart;

            report.append((probatronElapsed / 1000) + ";");

            if(probatronReport.contains("failed-assert")) {
                report.append("FAILURE;");
            } else {
                report.append("SUCCESS;");
            }

            toolLogs.append("probatron OUT:" + sep + "---" + sep + probatronReport + sep + sep);

            // Expand the .jp2 back to TIFF for pixel check using kakadu
            currentStage = "kdu_expand";
            CliCommand kdu_expand = new CliCommand(jp2, outtif);
            kdu_expand.runCommand("kdu_expand", "-i", "#infile#", "-o", "#outfile#");
            report.append(kdu_expand.getElapsedTime() + ";");
            report.append("SUCCESS;");

            toolLogs.append("kdu_expand OUT:" + sep + "---" + sep + kdu_expand.getStdOut() + sep + sep);
            toolLogs.append("kdu_expand ERR:" + sep + "---" + sep + kdu_expand.getStdErr() + sep + sep);

            // Compare the pixels of the .jp2 with the new TIFF
            currentStage = "gm";
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
            report.append("IOEXCEPTION in stage (" + currentStage + "): " + e.getMessage());
        } finally {

            tif.delete();
            jp2.delete();
            outtif.delete();
            profile.delete();
        }

        output.collect(new Text("TOOLLOG"), new Text(toolLogs.toString()));
        output.collect(filepath, new Text(report.toString()));
    }
}
