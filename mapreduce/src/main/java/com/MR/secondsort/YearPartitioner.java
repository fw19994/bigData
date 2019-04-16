package com.MR.secondsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<TmpComboKey, NullWritable> {
    @Override
    public int getPartition(TmpComboKey tmpComboKey, NullWritable nullWritable, int i) {
        System.out.print("yearPartitioner   "+tmpComboKey.getYear()+":"+ tmpComboKey.getTmp()+"   "+ tmpComboKey.getTmp()/i);
        return tmpComboKey.getYear()%i;
    }
}
