package com.avalonconsult.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Takes a list of doubles and outputs a summed total formatted to two decimal places
 */
public class CurrencyReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text output = new Text();
    private final DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text date, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        output.set(df.format(sum));
        context.write(date, output);
    }
}