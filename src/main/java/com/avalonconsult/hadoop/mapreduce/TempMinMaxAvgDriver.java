package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper;
import com.avalonconsult.hadoop.mapreduce.reducer.MinMaxAvgReducer;
import com.avalonconsult.hadoop.mapreduce.util.Cleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

import static com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper.MAPPER_TOKEN;

public class TempMinMaxAvgDriver extends Configured implements Tool {

    public static final String APP_NAME = TempMinMaxAvgDriver.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        final int status = ToolRunner.run(new Configuration(), new TempMinMaxAvgDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set(MAPPER_TOKEN, "\\s*,\\s*");
        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(TempMinMaxAvgDriver.class);

        // outputs for mapper and reducer
        job.setOutputKeyClass(Text.class);

        // setup mapper
        job.setMapperClass(TempMapper.class);
        job.setMapOutputValueClass(IntWritable.class);

        // setup reducer
        job.setReducerClass(MinMaxAvgReducer.class);
        job.setOutputValueClass(MinMaxAvgReducer.MinMaxAvgWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        final Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        Cleaner.clean(outputDir, conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class TempMapper extends TokenizerMapper<Text, IntWritable> {

        private final Text year = new Text();
        private final IntWritable value = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (value == null) return;
            String[] tokens = getTokens(context, value);

            if (tokens.length < 2) {
                System.err.printf("mapper: not enough records for %s%n", Arrays.toString(tokens));
                return;
            }

            int temp;
            try {
                temp = Integer.parseInt(tokens[1]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }

            this.year.set(tokens[0]);
            this.value.set(temp);

            context.write(this.year, this.value);

        }
    }
}
