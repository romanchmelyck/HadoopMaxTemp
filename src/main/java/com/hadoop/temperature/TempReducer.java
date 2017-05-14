package com.hadoop.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Roman on 14.05.2017.
 */
public class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE;
        for (IntWritable tempValue: values) {
            maxTemp = Math.max(maxTemp, tempValue.get());
        }
        context.write(key, new IntWritable(maxTemp));
    }

}
