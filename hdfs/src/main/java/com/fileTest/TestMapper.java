package com.fileTest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable outKey = new IntWritable();
        IntWritable outValue = new IntWritable();
        String[] values = value.toString().split(" ");
        System.out.println(values[0]+":"+values[1]);
        outKey.set(Integer.parseInt(values[0]));
        outValue.set(Integer.parseInt(values[1]));
        context.write(outKey,outValue);
    }

    public static void main(String[] args){
        String tmp = "201812  2011";
        String[] strings = tmp.split(" ");
        for(int i =0 ; i <strings.length ; i++){
            System.out.println(strings[i]);
        }

    }
}
