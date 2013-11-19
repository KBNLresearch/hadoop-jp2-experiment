package eu.scape_project.hadoop.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class LocalFile {
    private File file;

    public LocalFile(String filepath) throws IOException{
        file = new File(filepath);
        if(file.exists() && !file.canWrite()) {
            throw new IOException("cannot write to file in path: " + file.getAbsolutePath());
        }
    }

    public LocalFile(String filepath, String inpath, FileSystem fileSystem) throws IOException {
        this(filepath);
        fileSystem.copyToLocalFile(new Path(inpath), new Path(file.getAbsolutePath()));
    }

    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }

    public String getName() {
        return file.getName();
    }

    public void delete() {
        if(file != null && file.exists() && file.canWrite()) {
            file.delete();
        }
    }

    @Override
    public String toString() {
        return getAbsolutePath();
    }
}
