package com.atguigu.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建一个Job对象
        Configuration conf = new Configuration();

        //指定NameNode地址
        conf.set("fs.defaultFS","hdfs://hadoop102:9820");
        //执行运行要yarn上
        conf.set("mapreduce.framework.name","yarn");
        //执行resourcemanager地址
        conf.set("yarn.resourcemanager.hostname","hadoop103");
        //执行mr可以运行在远程集群上
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        //2. 关联jar
        //job.setJarByClass(WordCountDriver.class);
        job.setJar("D:\\IdeaProjects\\BigData200213\\MapReduceDEMO\\target\\MapReduceDEMO-1.0-SNAPSHOT.jar");
        //3. 关联Mapper 和 Reducer 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4. 设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6. 设置输入和输出路径
//        FileInputFormat.setInputPaths(job,new Path("D:/input/inputWord"));
//        FileOutputFormat.setOutputPath(job,new Path("D:/output1")); // 输出路径不能存在，如果已经存在就报异常.

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7. 提交job
        job.waitForCompletion(true);
    }
}
