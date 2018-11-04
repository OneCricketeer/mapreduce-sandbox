package com.avalonconsult.hadoop.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Utility class to clean up before runs
 */
public final class Cleaner {
    public static boolean clean(Path path, Configuration conf) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        return FileSystem.get(conf).delete(path, true);
    }
}
