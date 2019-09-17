package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.reducer.TemperatureReducer;
import com.avalonconsult.hadoop.mapreduce.util.Cleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WeatherMultiDriver extends Configured implements Tool {

    public static final String APP_NAME = WeatherMultiDriver.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        final int status = ToolRunner.run(new Configuration(), new WeatherMultiDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(WeatherMultiDriver.class);
        job.setJobName("Hot or Cold Day Job");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // dataset passed as command line argument 0 and output path as argument 1
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MinTempMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MaxTempMapper.class);

        job.setReducerClass(TemperatureReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        final Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        Cleaner.clean(outputDir, conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class MinTempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private Text text = new Text();
        private FloatWritable floatWritable = new FloatWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

            while (tokenizer.hasMoreElements()) {

                String[] split = tokenizer.nextToken().split("\\s+");

                if (!split[6].equals("-9999.0")) {

                    text.set(split[1]);
                    floatWritable.set(Float.parseFloat(split[6]));
                    context.write(text, floatWritable);
                }
            }
        }
    }

    static class MaxTempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private Text text = new Text();
        private FloatWritable floatWritable = new FloatWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

            while (tokenizer.hasMoreElements()) {

                String[] split = tokenizer.nextToken().split("\\s+");

                if (!split[5].equals("-9999.0")) {

                    text.set(split[1]);
                    floatWritable.set(Float.parseFloat(split[5]));
                    context.write(text, floatWritable);
                }
            }
        }
    }

}
