package com.avalonconsult.hadoop.mapreduce.recordreader.pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternRecordReader extends RecordReader<LongWritable, Text> {

    public static final String RECORD_DELIMITER = "record.delimter.pattern";
    public static final String RECORD_DELIMITER_KEEP = "record.delimter.keep-pattern";

    private static final Log LOG = LogFactory.getLog(PatternRecordReader.class);

    private LineReader in;
    private static final Text EOL = new Text("\n");
    private Pattern delimiterPattern;
    private String delimiterRegex;
    private Boolean keepPattern;
    private int maxLineLength;
    private long start, end;
    private long pos;
    private Text value = new Text();
    private LongWritable key = new LongWritable();

    private Text headerMatch = new Text();

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration job = context.getConfiguration();
        this.delimiterRegex = job.get(RECORD_DELIMITER);
        this.keepPattern =  job.getBoolean(RECORD_DELIMITER_KEEP, false);

        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength",
                Integer.MAX_VALUE);
        delimiterPattern = Pattern.compile(delimiterRegex);

        FileSplit split = (FileSplit) genericSplit;
        this.start = split.getStart();
        this.end = this.start + split.getLength();

        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);

        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Make sure no line is missed
            --start;
            fileIn.seek(start);
        }

        in = new LineReader(fileIn, job);

        if (skipFirstLine) {
            Text tmp = new Text();
            start += in.readLine(tmp, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE,
                            end - start));
        }

        this.pos = start;
    }

    private int readNext(Text text,
                         int maxLineLength,
                         int maxBytesToConsume)
            throws IOException{
        int offset = 0;
        text.clear();
        Text tmp = new Text();

        for (int i = 0; i < maxBytesToConsume; i++) {
            int offsetTemp = in.readLine(tmp, maxLineLength, maxBytesToConsume);
            offset += offsetTemp;

            Matcher m  = delimiterPattern.matcher(tmp.toString());

            // End of File
            if (offsetTemp == 0) {
                break;
            }

            if (m.matches()) {
                // Record delimiter
                String matched = m.group();
                headerMatch.set(matched.getBytes(), 0, matched.length());
                break;
            } else {
                // Append value to record
                text.append(EOL.getBytes(), 0, EOL.getLength());

                if (keepPattern && headerMatch.getLength() != 0) {
                    text.append(headerMatch.getBytes(), 0, headerMatch.getLength());
                    text.append(EOL.getBytes(), 0, EOL.getLength());
                    headerMatch.clear();
                }

                text.append(tmp.getBytes(), 0, tmp.getLength());
            }
        }
        return offset;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        key.set(pos);

        int newSize = 0;

        while (pos < end) {
            // Read first record , store it to value
            newSize = readNext(value, maxLineLength,
                    Math.max( (int) Math.min(
                            Integer.MAX_VALUE, end - pos),
                            maxLineLength));
            // no bytes read, end of split
            // break and return false
            if (newSize == 0) {
                break;
            }
            // record has been read
            pos += newSize;

            // found key/value, break
            if (newSize < maxLineLength) {
                break;
            }

            // Line too long
            // Try again with position += line offset
            String fmt = "Skipped line of size %d at pos %d";
            LOG.info(String.format(fmt, newSize, (pos - newSize)));
        }

        if (newSize == 0) {
            // end of split
            key = null;
            value = null;
            return false;
        } else {
            // return true: record found
            // continue to retrieve next key/value
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException,
            InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, pos - start / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
