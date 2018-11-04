package com.avalonconsult.hadoop.mapreduce.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Objects;

/**
 * Reducer to calculate integer values min, max, and avg.
 * Encapsulates the reducer logic into a custom Writable object.
 * Output is determined by Writable toString implementation.
 */
public class MinMaxAvgReducer extends Reducer<Text, IntWritable, Text, MinMaxAvgReducer.MinMaxAvgWritable> {

    private final MinMaxAvgWritable output = new MinMaxAvgWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        output.set(values);
        context.write(key, output);
    }

    public static class MinMaxAvgWritable implements Writable {

        private int min, max;
        private double avg;

        private DecimalFormat df = new DecimalFormat("#.00");

        public void set(Iterable<IntWritable> values) {
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;

            int sum = 0;
            int count = 0;
            for (IntWritable iw : values) {
                int i = iw.get();
                if (i < this.min) this.min = i;
                if (i > max) this.max = i;
                sum += i;
                count++;
            }

            this.avg = count <= 1 ? sum : (sum / (1.0 * count));
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(min);
            dataOutput.writeInt(max);
            dataOutput.writeDouble(avg);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.min = dataInput.readInt();
            this.max = dataInput.readInt();
            this.avg = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return "MinMaxAvgWritable{" +
                    "min=" + min +
                    ", max=" + max +
                    ", avg=" + df.format(avg) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MinMaxAvgWritable that = (MinMaxAvgWritable) o;
            return min == that.min &&
                    max == that.max &&
                    avg == that.avg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, max, avg);
        }
    }
}