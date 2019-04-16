package com.fileTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestApp {
    public static void main(String[] args) throws Exception{
        //得到配置对象，如果有配置文件，系统会自动读取配置文件
        Configuration conf = new Configuration();
        //通过配置对象来设置系统配置，如配置文件系统
        conf.set("fs.defaultFS","file:///");
        //通过配置对象得到相应的job
        Job job = Job.getInstance(conf);
        //配置工作对象的属性，名称，等等。
        job.setJobName("testJob");
        job.setJarByClass(TestApp.class);
        job.setInputFormatClass(TextInputFormat.class);
        //添加输入路径和输出路径
        FileInputFormat.addInputPath(job,new Path("F:\\dataTest\\TestFile.txt"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\dataTest\\out"));
        //设置对应的map和reduce类
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        //设置对应map和reduce的输出格式
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce个数
        job.setNumReduceTasks(3);
        job.waitForCompletion(true);
    }
}
