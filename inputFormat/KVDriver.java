package com.atguigu.mr.inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建一个Job对象
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        Job job = Job.getInstance(conf);
        //2. 关联jar
        job.setJarByClass(KVDriver.class);
        //3. 关联Mapper 和 Reducer 类
        job.setMapperClass(KVMapper.class);
        job.setReducerClass(KVReducer.class);
        //4. 设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置使用KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputkv"));
        FileOutputFormat.setOutputPath(job,new Path("D:/output8")); // 输出路径不能存在，如果已经存在就报异常.

        //7. 提交job
        job.waitForCompletion(true);
    }
}
