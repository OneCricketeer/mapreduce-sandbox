package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper;
import com.avalonconsult.hadoop.mapreduce.util.Cleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

import static com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper.MAPPER_TOKEN;

public class CustomerDriver extends Configured implements Tool {

    public static final String APP_NAME = CustomerDriver.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        final int status = ToolRunner.run(new Configuration(), new CustomerDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set(MAPPER_TOKEN, "\\|");
        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(CustomerDriver.class);

        job.setMapperClass(NationBalanceMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(AverageReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        final Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        Cleaner.clean(outputDir, conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class NationBalanceMapper extends TokenizerMapper<Text, DoubleWritable> {

        private final Text keyOut = new Text();
        private final DoubleWritable valueOut = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = getTokens(context, value);

            if (tokens.length < 7) {
                System.err.printf("mapper: not enough records for %s", Arrays.toString(tokens));
                return;
            }
            // String custKey = tokens[1];

            int nation = 0;
            double balance = 0;
            try {
                nation = Integer.parseInt(tokens[3]);
                balance = Double.parseDouble(tokens[5]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }

            // filter large balances and nations between 1-14
            if (balance > 8000 && (nation < 15 && nation > 1)) {
                keyOut.set(tokens[6]);
                // valueOut.set(custKey + "::" + balance); // create simple composite value
                valueOut.set(balance);

                context.write(keyOut, valueOut);
            }
        }
    }

    static class AverageReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private final Text output = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sumBalance = 0;
            int count = 0;

            for (DoubleWritable v : values) {
                // String[] a = v.toString().trim().split("::");

                sumBalance += v.get();
                count++;
            }

            double avgBalance = count <= 1 ? sumBalance : (sumBalance / count);

            output.set(count + "\t" + avgBalance);
            context.write(key, output);
        }
    }
}
