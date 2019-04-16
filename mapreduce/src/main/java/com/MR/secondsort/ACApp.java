package com.MR.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ACApp {
    public static void main(String[] args){
        Configuration conn = new Configuration();
        conn.set("fs.defaultFS","file:///");
        Job job = null;
        try {
            job = Job.getInstance(conn);
            job.setJobName("test");
            job.setJarByClass(ACApp.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,new Path("F:/dataTest/TestFile.txt"));
            FileOutputFormat.setOutputPath(job,new Path("F:/dataTest/sort/out"));
            job.setMapperClass(TmpMapper.class);
            job.setReducerClass(TmpReducer.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(TmpComboKey.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            job.setPartitionerClass(YearPartitioner.class);
            job.setGroupingComparatorClass(YearGroupComparator.class);
            job.setSortComparatorClass(ComboKeyComparator.class);
            job.setNumReduceTasks(3);
            job.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
