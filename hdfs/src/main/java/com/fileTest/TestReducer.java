package com.fileTest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    IntWritable mabValue = new IntWritable(0);
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for(IntWritable s:values) {
            System.out.println(key+"::::::"+s.get());
            if (s.compareTo(mabValue) > 0) {
                mabValue.set(s.get());
            }
        }
        context.write(key,mabValue);
    }
}
