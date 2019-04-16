package com.MR.mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlMaper extends Mapper<LongWritable,UserDBWritable,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, UserDBWritable value, Context context) throws IOException, InterruptedException {
        System.out.println("MApper longWriteable="+key);
        Text out = new Text();
        out.set(value.getName()+value.getAddress());
        context.write(out,new IntWritable(1));
    }
}
