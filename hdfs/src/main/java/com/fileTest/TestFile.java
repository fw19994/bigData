package com.fileTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFile {

    /**
     *
     * @throws Exception
     */
    @Test
    public void writeFile () throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem system = FileSystem.get(conf);
        FSDataOutputStream out = system.create(new Path("F:\\dataTest\\TestFile.txt"));
        String data = "";
        for(int i = 0 ; i<100 ; i++){
            int tmp = (int)(Math.random()*12)+1;
            if(tmp <10){
                data += "2018"+"0"+tmp+" ";
            }else{
                data += "2018"+tmp+" ";
            }
            data += (int)(Math.random()*100)+"\n";
        }
        out.write(data.toString().getBytes());
        out.close();
    }

}
