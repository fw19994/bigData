package com.MR.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组对比器，（正常的分组是按照key相同分为一组）
 */
public class YearGroupComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TmpComboKey tmpComboKey1 = (TmpComboKey) a;
        TmpComboKey tmpComboKey2 = (TmpComboKey)b;
        return tmpComboKey1.getYear()-tmpComboKey2.getTmp();
    }
}
