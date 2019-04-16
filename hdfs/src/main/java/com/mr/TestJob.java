package com.mr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TestJob  {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://39.106.51.31:8020/");
        //conf.set("fs.defaultFS","file:///");
        //建立任务
        Job job= Job.getInstance(conf);
        //设置最大切片数
        //FileInputFormat.setMaxInputSplitSize(job,13);
        //最小切片数
        //FileInputFormat.setMinInputSplitSize(job,1L);

//        //设置分区类
//        job.setPartitionerClass(.class);   //设置自定义分区

        //设置合成类
       // job.setCombinerClass(TestReducer.class);          //设置combiner类
        //设置任务map
        job.setMapperClass(TestMap.class);
        //搜索类
        job.setJarByClass(TestJob.class);
        //设置任务reducer
        job.setReducerClass(TestReducer.class);
        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        //设置输入文件路径
        TextInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出文件路径
        TextOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置任务名
        job.setJobName("testJob");
        //设置reducer的数量
        job.setNumReduceTasks(3);
        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }

}
