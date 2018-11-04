package com.avalonconsult.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class TokenizerMapper<K_OUT, V_OUT> extends Mapper<LongWritable, Text, K_OUT, V_OUT> {

    public static final String MAPPER_TOKEN = "mapper.token";

    protected String[] getTokens(Context c, Text value) {
        String token = c.getConfiguration().get(MAPPER_TOKEN);
        return value.toString().trim().split(token);
    }
}