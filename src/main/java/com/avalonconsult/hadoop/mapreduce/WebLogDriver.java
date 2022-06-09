package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.util.Cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebLogDriver extends Configured implements Tool {

  public static final String APP_NAME = WebLogDriver.class.getSimpleName();

  public static void main(String[] args) throws Exception {
    final int status = ToolRunner.run(new Configuration(), new WebLogDriver(), args);
    System.exit(status);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, APP_NAME);
    job.setJarByClass(WebLogDriver.class);

    // outputs for mapper and reducer
    job.setOutputKeyClass(Text.class);

    // setup mapper
    job.setMapperClass(WebLogDriver.WebLogMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    // setup reducer
    job.setReducerClass(WebLogDriver.WebLogReducer.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    final Path outputDir = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputDir);
    Cleaner.clean(outputDir, conf);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  static class WebLogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    static final Pattern HTTP_LOG_PATTERN = Pattern.compile("(\\S+) - - \\[(.+)] \"(\\S+) (/\\S*) HTTP/\\S+\" \\S+ (\\d+)");

    final Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if (line.isEmpty()) {
        return;
      }
      Matcher matcher = HTTP_LOG_PATTERN.matcher(line);

      if (matcher.matches()) {
        String url = matcher.group(1);
        try {
          valueOut.set(String.format("%s,%d", url, 1));

          context.write(NullWritable.get(), valueOut);
        } catch (NumberFormatException e) {
          e.printStackTrace();
        }
      }
    }
  }

  static class WebLogReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

    static final Text keyOut = new Text();
    static final IntWritable valueOut = new IntWritable();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      Map<String, Integer> m = new TreeMap<>();
      for (Text t : values) {
        String v = new String(t.getBytes(), StandardCharsets.UTF_8);
        String[] parts = v.split(",");

        String host = parts[0];
        int count = m.getOrDefault(host, 0) + Integer.parseInt(parts[1]);
        m.put(host, count);
      }

      for (Map.Entry<String, Integer> e : m.entrySet()) {
        keyOut.set(e.getKey());
        valueOut.set(e.getValue());
        context.write(keyOut, valueOut);
      }
    }
  }
}
