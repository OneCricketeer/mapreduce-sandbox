package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper;
import com.avalonconsult.hadoop.mapreduce.reducer.CurrencyReducer;
import com.avalonconsult.hadoop.mapreduce.util.Cleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class StoreSumDriver extends Configured implements Tool {

    public static final String APP_NAME = StoreSumDriver.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        final int status = ToolRunner.run(new Configuration(), new StoreSumDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set(MAPPER_TOKEN, "\\s\\s+");
        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(StoreSumDriver.class);

        job.setMapperClass(StoreMapper.class);
        job.setReducerClass(CurrencyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        final Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        Cleaner.clean(outputDir, conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class StoreMapper extends TokenizerMapper<Text, DoubleWritable> {

        private final Text key = new Text();
        private final DoubleWritable sales = new DoubleWritable();

        @Override
        protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {

            String[] data = getTokens(context, value);

            if (data.length < 6) {
                System.err.printf("mapper: not enough records for %s%n", Arrays.toString(data));
                return;
            }

            final String product = data[3];
            key.set(product);

            try {
                sales.set(Double.parseDouble(data[4]));
                context.write(key, sales);
            } catch (NumberFormatException ex) {
                System.err.printf("mapper: invalid value format %s%n", data[4]);
            }
        }
    }
}
