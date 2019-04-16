package com.MR.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MysqlApp {
    public static void main(String[] args){
        Configuration conn = new Configuration();
        conn.set("dfs.defulat","file:///");
        Job job = null;
        try {
            job = Job.getInstance(conn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJobName("MysqlTest");
        job.setJarByClass(MysqlApp.class);
        //连接数据库的信息
        String driverClass = "com.mysql.jdbc.Driver";
        String mysqlUrl = "jdbc:mysql://39.106.51.31:3306/fengwei";
        String name = "fengwei";
        String password = "317812";
        DBConfiguration.configureDB(job.getConfiguration(),driverClass,mysqlUrl,name,password);
        DBInputFormat.setInput(job,UserDBWritable.class,"select userName,address from USER","select count(*) from USER");
        FileOutputFormat.setOutputPath(job,new Path("F://dataTest/mysql/out"));
        job.setMapperClass(MysqlMaper.class);
        job.setReducerClass(MysqlReduce.class);
        job.setNumReduceTasks(3);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        try {
            job.waitForCompletion(false);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
