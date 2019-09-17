package com.avalonconsult.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TemperatureReducer extends Reducer<Text, FloatWritable, Text, Text> { // line no 10 according to stacktrace

    private Text text = new Text();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<FloatWritable> iterator = values.iterator();

        while (iterator.hasNext()) {

            float temperature = iterator.next().get();

            if (temperature > 40) {

                text.set("Hot Day");
                context.write(key, text);

            } else if (temperature < 10) {

                text.set("Cold Day");
                context.write(key, text); // line 31 according to stacktrace
            }
        }
    }
}