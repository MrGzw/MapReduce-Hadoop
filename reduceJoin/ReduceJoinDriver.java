package com.atguigu.mr.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2. 关联jar
        job.setJarByClass(ReduceJoinDriver.class);
        //3. 关联mapper和reducer
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        //4. 设置map输出的k 和 v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        //5. 设置最终输出的k 和 v的类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:/input/inputtable"));
        FileOutputFormat.setOutputPath(job,new Path("D:/output"));

        //7. 提交job
        job.waitForCompletion(true);
    }
}
