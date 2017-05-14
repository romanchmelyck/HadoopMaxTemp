package com.hadoop.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Roman on 14.05.2017.
 */
public class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
//        System.out.println(line);
        if (line.contains("TMAX")) {
            String[] cols = line.split(",");
            String year = cols[1].substring(0, 4);
//            System.out.println(year);
            int temperature;
            if (cols[3].length() != 0) {
                temperature = Integer.parseInt(cols[3]);
                context.write(new Text(year), new IntWritable(temperature));
            }
        }
    }
}
