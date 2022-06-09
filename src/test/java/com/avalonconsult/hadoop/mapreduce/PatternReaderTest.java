package com.avalonconsult.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;

import com.avalonconsult.hadoop.mapreduce.recordreader.pattern.PatternInputFormat;
import com.avalonconsult.hadoop.mapreduce.recordreader.pattern.PatternRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class PatternReaderTest extends LocalFileReaderTest {

  private static final String PATTERN = "^[A-Za-z]{3},\\s\\d{2}\\s[A-Za-z]{3}\\s\\d{4,}\\s\\d{2}:\\d{2}:\\d{2}";
  private static final int RECORD_COUNT = 3;

  @Test
  public void testHeader() throws IOException, InterruptedException {

    PatternRecordReader reader = getPatternRecordReader("pattern/header.txt");

    int recordCount = 0;
    while (reader.nextKeyValue()) {
      Text t = reader.getCurrentValue();
      if (t.getLength() != 0) {
        recordCount++;
      }
    }

    assertEquals(RECORD_COUNT, recordCount);
  }

  private PatternRecordReader getPatternRecordReader(String file) throws IOException, InterruptedException {
    String resource = getResource(file).getPath();
    return getPatternRecordReader(conf, PATTERN, getFileSplit(resource));
  }

  @Test
  @Ignore
  public void testPatternInside() throws IOException, InterruptedException {

    PatternRecordReader reader = getPatternRecordReader("pattern/in_record.txt");

    int recordCount = 0;
    while (reader.nextKeyValue()) {
      Text t = reader.getCurrentValue();
      if (t.getLength() != 0) {
        recordCount++;
      }
    }

    assertEquals(RECORD_COUNT, recordCount);

  }

  @Test
  public void testPatternHeadAndInside() throws IOException, InterruptedException {

    PatternRecordReader reader = getPatternRecordReader("pattern/header_and_record.txt");

    int recordCount = 0;
    while (reader.nextKeyValue()) {
      Text t = reader.getCurrentValue();
      if (t.getLength() != 0) {
        recordCount++;
      }
    }

    assertEquals(RECORD_COUNT, recordCount);

  }


  /*
   * Utility functions
   */

  private PatternRecordReader getPatternRecordReader(Configuration conf, String pattern, FileSplit split) throws IOException, InterruptedException {

    conf.set(PatternRecordReader.RECORD_DELIMITER, pattern);

    PatternInputFormat inputFormat = ReflectionUtils.newInstance(PatternInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    PatternRecordReader reader = (PatternRecordReader) inputFormat.createRecordReader(split, context);
    reader.initialize(split, context);
    return reader;
  }

}
