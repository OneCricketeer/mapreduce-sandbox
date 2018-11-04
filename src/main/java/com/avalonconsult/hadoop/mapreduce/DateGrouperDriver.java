package com.avalonconsult.hadoop.mapreduce;

import com.avalonconsult.hadoop.mapreduce.reducer.ValueCounterReducer;
import com.avalonconsult.hadoop.mapreduce.util.Cleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper.MAPPER_TOKEN;

public class DateGrouperDriver extends Configured implements Tool {

    public static final String APP_NAME = DateGrouperDriver.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        final int status = ToolRunner.run(new Configuration(), new DateGrouperDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set(MAPPER_TOKEN, ",");
        conf.setBoolean(ValueCounterReducer.VALUE_COUNTER_SORT_KEYS, true);
        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(DateGrouperDriver.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ValueCounterReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        final Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        Cleaner.clean(outputDir, conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Reads CSV data up to only first comma, then treats the rest as a value.
     * It is assumed first column is in yyyy-MM-dd format, and the values are not parsed.
     */
    static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text key = new Text();
        private final Text segment = new Text();

        @Override
        protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {

            int separatorIndex = value.find(context.getConfiguration().get(MAPPER_TOKEN, ","));

            final String valueStr = value.toString();
            if (separatorIndex < 0) {
                System.err.printf("mapper: not enough records for %s", valueStr);
                return;
            }
            String dateKey = valueStr.substring(0, separatorIndex).trim();
            String tokens = valueStr.substring(1 + separatorIndex).trim().replaceAll("\\p{Space}", "");
            segment.set(tokens);

            SimpleDateFormat fmtFrom = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat fmtTo = new SimpleDateFormat("yyyy-MM");

            try {
                dateKey = fmtTo.format(fmtFrom.parse(dateKey));
                // System.out.println(dateKey);
                key.set(dateKey);
            } catch (ParseException ex) {
                System.err.printf("mapper: invalid key format %s", dateKey);
                return;
            }

            context.write(key, segment);
        }
    }
}
