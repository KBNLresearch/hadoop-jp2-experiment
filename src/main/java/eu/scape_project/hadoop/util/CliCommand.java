package eu.scape_project.hadoop.util;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

public class CliCommand {
    private LocalFile infile = null;
    private LocalFile outfile = null;
    private StringBuffer stdout = new StringBuffer();
    private StringBuffer stderr = new StringBuffer();
    private Long elapsedTime;

    public CliCommand(LocalFile infile) {
        this.infile = infile;
    }

    public CliCommand(LocalFile infile, LocalFile outfile) {
        this(infile);
        this.outfile = outfile;
    }

    /**
     *
     * @param command the command line tool
     * @param args list of arguments (should at least include the exact strings "#infile#" and "#outfile#.ext",
     *             where ".ext" can be any extension)
     */
    public void runCommand(String command, String... args) throws IOException {
        Executor exec = new DefaultExecutor();
        CommandLine cl = new CommandLine(command);
        for(String arg : args) {
            if(arg.matches("^#infile#.*$")) { arg = arg.replaceAll("#infile#", infile.getPath()); }
            else if(arg.matches("^#outfile#.*$")) { arg = arg.replaceAll("#outfile#", outfile.getPath()); }
            cl.addArgument(arg);
        }
        exec.setStreamHandler(new PumpStreamHandler(new StdStrHandler(stdout), new StdStrHandler(stderr)));
        long start_time = System.nanoTime();
        exec.execute(cl);
        elapsedTime = (long) (Math.ceil(System.nanoTime() - start_time)/1e6);
    }

    public String getStdOut() {
        return stdout.toString();
    }

    public String getStdErr() {
        return stderr.toString();
    }

    public Long getElapsedTime() {
        return elapsedTime;
    }

    private class StdStrHandler extends LogOutputStream {
        StringBuffer sb;
        public StdStrHandler(StringBuffer sb) {
            this.sb = sb;
        }

        @Override
        protected void processLine(String s, int i) {
            sb.append(s).append(System.getProperty("line.separator"));
        }
    }
}
