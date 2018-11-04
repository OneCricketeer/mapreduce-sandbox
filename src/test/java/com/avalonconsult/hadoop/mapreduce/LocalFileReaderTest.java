package com.avalonconsult.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

public class LocalFileReaderTest {
    protected Configuration conf;

    @Before
    public void init() {
        conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
    }

    protected URL getResource(String name) throws IOException {
        URL resource = getClass().getClassLoader().getResource(name);
        if (resource == null) {
            throw new IOException("failed to read file");
        }
        return resource;
    }

    protected FileSplit getFileSplit(String filePath) {
        return getFileSplit(Paths.get(filePath).toFile());
    }

    protected FileSplit getFileSplit(File f) {
        Path path = new Path(f.getAbsoluteFile().toURI());
        return new FileSplit(path, 0, f.length(), null);
    }
}
