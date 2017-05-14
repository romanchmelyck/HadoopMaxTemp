package com.hadoop.temperature;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by Roman on 14.05.2017.
 */
public class MaxTempJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        FileUtils.deleteDirectory(new File("jobResults"));

        String inputPath = "input";
        String outPutPath = "jobResults";

        Job job = new Job();
        job.setJarByClass(MaxTempJob.class);
        job.setJobName("Max temp job");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPutPath));

        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
