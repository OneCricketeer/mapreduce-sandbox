package com.avalonconsult.hadoop.mapreduce.reducer;

import net.minidev.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static com.avalonconsult.hadoop.mapreduce.mapper.TokenizerMapper.MAPPER_TOKEN;

/**
 * Takes values and outputs a counted collection of them in JSON format
 */
public class ValueCounterReducer extends Reducer<Text, Text, Text, Text> {

    public static final String VALUE_COUNTER_SORT_KEYS = "value.counter.reducer.sort.keys";

    private final Text output = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // re-use token separator from Driver
        final Configuration configuration = context.getConfiguration();
        String delimiter = configuration.get(MAPPER_TOKEN, ",");
        boolean sortKeys = configuration.getBoolean(VALUE_COUNTER_SORT_KEYS, false);

        // Use TreeMap natural ordering to sort keys
        Map<String, Integer> keyMap = new TreeMap<>();
        for (Text v : values) {
            String[] parts = String.valueOf(v).trim().split(delimiter);

            for (String p : parts) {
                if (!keyMap.containsKey(p)) {
                    keyMap.put(p, 0);
                }
                keyMap.put(p, 1 + keyMap.get(p));
            }
        }

        output.set(sortKeys ? mapToString(keyMap) : mapToJSON(keyMap));
        context.write(key, output);
    }

    private String mapToJSON(Map<String, Integer> map) {
        // JSON objects don't preserve map order
        return new JSONObject(map).toString();
    }

    /**
     * Outputs map as JSON string with sorted keys
     * @param map
     * @return JSON string with sorted keys
     */
    private String mapToString(Map<String, Integer> map) {
        StringBuilder sb = new StringBuilder("{");
        String delimiter = ",";
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(
                    String.format("\"%s\":%d", entry.getKey(), entry.getValue())
            ).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());
        return sb.append("}").toString();
    }
}