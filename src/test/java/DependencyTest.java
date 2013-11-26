import eu.scape_project.hadoop.ConversionRunner;
import eu.scape_project.hadoop.util.CliCommand;
import eu.scape_project.hadoop.util.LocalFile;
import org.junit.Test;
import org.probatron.Driver;
import org.probatron.ValidationReport;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

public class DependencyTest {

    @Test
    public void testForKakadu() throws IOException {
/*        String jp2 = ConversionRunner.class.getResource("/test.jp2").getFile();
        LocalFile tmptif = new LocalFile("tmp.tif");
        CliCommand cmd = new CliCommand(new LocalFile(jp2), tmptif);
        cmd.runCommand("kdu_expand", "-i", "#infile#", "-o", "#outfile#");
        assert(new File(tmptif.getAbsolutePath()).exists());
        tmptif.delete();*/
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

        String schema = "file:" + ConversionRunner.class.getResource("/kbMaster.sch").getFile();
        String doc = "file:" + ConversionRunner.class.getResource("/pyly.xml").getFile();
        String goodDoc = "file:" + ConversionRunner.class.getResource("/pyly_good.xml").getFile();

        org.probatron.Session ses = new org.probatron.Session();
        ses.setSchemaDoc(schema);
        ValidationReport report = ses.doValidation(doc);
		ValidationReport report_good = ses.doValidation(goodDoc);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        report.streamOut(baos);
        assert(baos.toString().contains("failed-assert"));
		baos.close();
		baos = new ByteArrayOutputStream();
		report_good.streamOut(baos);
		assert(!baos.toString().contains("failed-assert"));
		baos.close();
    }

	@Test
	public void testForAware() throws IOException {
		String awareOpts = ConversionRunner.class.getResource("/optionsKBMasterLossless.xml").getFile();
        String tif = ConversionRunner.class.getResource("/test.tif").getFile();
        LocalFile tmpjp2 = new LocalFile("tmp.jp2");
        new CliCommand(new LocalFile(tif), tmpjp2)
			.runCommand("jpwrappa.py",  "#infile#", "#outfile#", "-p", awareOpts);
        assert(new File(tmpjp2.getAbsolutePath()).exists());
		tmpjp2.delete();
	}
}
