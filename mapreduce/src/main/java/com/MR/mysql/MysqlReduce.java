package com.MR.mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MysqlReduce extends Reducer<Text,IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable size = new IntWritable(0);
        int size1 = 0;
        for(IntWritable o:values){
            size1=size1+(int)o.get();
        }
        size.set(size1);
        context.write(key,size);
    }
}
