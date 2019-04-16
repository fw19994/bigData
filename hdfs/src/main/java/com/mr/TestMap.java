package com.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @author  fengwei
 */
public class TestMap extends Mapper<LongWritable, Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();
        String[] words = value.toString().split(" ");
        for(String word : words){
            keyOut.set(word);
            valueOut.set(1);
            context.write(keyOut,valueOut);
        }
    }
}
