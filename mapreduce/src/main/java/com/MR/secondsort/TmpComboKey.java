package com.MR.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TmpComboKey implements WritableComparable<TmpComboKey> {
    private int year;
    private int tmp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTmp() {
        return tmp;
    }

    public void setTmp(int tmp) {
        this.tmp = tmp;
    }

    @Override
    public int compareTo(TmpComboKey o) {

        if(year == o.getYear()){
            if( tmp == o.getTmp()){
                return 0;
            }else{
                return -(tmp - o.getTmp());

            }
        }else {
            return (year - o.getYear());
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(tmp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        tmp =  dataInput.readInt();
    }
}
