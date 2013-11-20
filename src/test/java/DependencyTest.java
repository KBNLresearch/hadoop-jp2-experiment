import eu.scape_project.hadoop.ConversionRunner;
import eu.scape_project.hadoop.util.CliCommand;
import eu.scape_project.hadoop.util.LocalFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class DependencyTest {

    @Test
    public void testForKakadu() throws IOException {
        String jp2 = ConversionRunner.class.getResource("/test.jp2").getFile();
        LocalFile tmptif = new LocalFile("tmp.tif");
        CliCommand cmd = new CliCommand(new LocalFile(jp2), tmptif);
        cmd.runCommand("kdu_expand", "-i", "#infile#", "-o", "#outfile#");
        assert(new File(tmptif.getAbsolutePath()).exists());
        tmptif.delete();
    }

    @Test
    public void testForOpjCompress() throws IOException {
        String tif = ConversionRunner.class.getResource("/test.tif").getFile();
        LocalFile tmpjp2 = new LocalFile("tmp.jp2");
        new CliCommand(new LocalFile(tif), tmpjp2)
                .runCommand("opj_compress", "-n", "6", "-t", "512,512", "-i", "#infile#", "-o", "#outfile#");
        assert(new File(tmpjp2.getAbsolutePath()).exists());
        tmpjp2.delete();
    }

    @Test
    public void testForJpylyzer() throws IOException {
        String jp2 = ConversionRunner.class.getResource("/test.jp2").getFile();
        CliCommand cmd = new CliCommand(new LocalFile(jp2));
        cmd.runCommand("jpylyzer", "#infile#");
        assert(cmd.getStdOut().contains("<isValidJP2>True</isValidJP2>"));
    }

    @Test
    public void testForProbatron() throws IOException {
        String probatron = ConversionRunner.class.getResource("/external-tools/probatron.jar").getFile();
        String schema = ConversionRunner.class.getResource("/kbMaster.sch").getFile();
        String doc = ConversionRunner.class.getResource("/pyly.xml").getFile();
        CliCommand cmd = new CliCommand(new LocalFile(doc));
        cmd.runCommand("java", "-jar", probatron, "#infile#", schema);
        assert(cmd.getStdOut().contains("failed-assert"));
    }
}
