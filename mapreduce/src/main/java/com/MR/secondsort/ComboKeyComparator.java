package com.MR.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComboKeyComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TmpComboKey tmpCombokey1 = (TmpComboKey)a;
        TmpComboKey tmpComboKey2 = (TmpComboKey)b;
        return tmpCombokey1.compareTo(tmpComboKey2);
    }
}
