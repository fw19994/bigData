package com.MR.secondsort;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TmpReducer extends Reducer<TmpComboKey, NullWritable, IntWritable,IntWritable> {
    @Override
    protected void reduce(TmpComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("TemReduer key ="+ key.getTmp()+" " +key.getYear()+" values=:"+values);
        int year = key.getYear();
        int tmp = key.getTmp();
        for(NullWritable a: values){
            System.out.println(key.getYear()+":" +key.getTmp());
        }
        context.write(new IntWritable(year),new IntWritable(tmp));
    }
}
