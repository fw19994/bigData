package com.MR.secondsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TmpMapper extends Mapper<LongWritable, Text,TmpComboKey, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         System.out.println("Mapper  key=" +key+"  value="+value);
         String[] ar = value.toString().split(" ");
         TmpComboKey tmpComboKey = new TmpComboKey();
         tmpComboKey.setYear(Integer.parseInt(ar[0]));
         tmpComboKey.setTmp(Integer.parseInt(ar[1]));
         context.write(tmpComboKey,NullWritable.get());
    }
}
