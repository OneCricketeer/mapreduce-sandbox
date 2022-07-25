package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.util.Cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

public class TokenizedLineDriver extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(TokenizedLineDriver.class);

  public static final String APP_NAME = TokenizedLineDriver.class.getSimpleName();

  public static void main(String[] args) throws Exception {
    final int status = ToolRunner.run(new Configuration(), new TokenizedLineDriver(), args);
    System.exit(status);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, APP_NAME);
    job.setJarByClass(TokenizedLineDriver.class);

    // outputs for mapper and reducer
    job.setOutputKeyClass(Text.class);

    // setup mapper
    job.setMapperClass(Mapper.class);
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(Text.class);

    // setup reducer
    job.setReducerClass(Reducer.class);
    job.setOutputValueClass(ByteWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    final Path outputDir = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputDir);
    Cleaner.clean(outputDir, conf);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, ByteWritable, Text> {

    final ByteWritable keyOut = new ByteWritable();
    final Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, ByteWritable, Text>.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if (line.isEmpty()) {
        return;
      }

      StringTokenizer tokenizer = new StringTokenizer(line, ";");
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        String[] parts = token.split(",");
        String keyStr = parts[2];
        if (keyStr.matches("[ab]")) {
          LOG.info("key={}, value={}", keyStr, token);
          keyOut.set((byte) keyStr.charAt(0));
          valueOut.set(token);
          context.write(keyOut, valueOut);
        }
      }
    }
  }

  static class Reducer extends org.apache.hadoop.mapreduce.Reducer<ByteWritable, Text, Text, LongWritable> {

    static final Text keyOut = new Text();
    static final LongWritable valueOut = new LongWritable();

    @Override
    protected void reduce(ByteWritable key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<ByteWritable, Text, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      keyOut.set(new String(new byte[]{key.get()}, StandardCharsets.UTF_8));
      valueOut.set(StreamSupport.stream(values.spliterator(), true)
                       .mapToLong(v -> 1).sum());
      context.write(keyOut, valueOut);
    }
  }
}
