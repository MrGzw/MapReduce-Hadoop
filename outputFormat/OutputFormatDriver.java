package com.atguigu.mr.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputFormatDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2. 关联jar
        job.setJarByClass(OutputFormatDriver.class);
        //3. 关联mapper和reducer
        job.setMapperClass(OutputFormatMapper.class);
        job.setReducerClass(OutputFormatReducer.class);
        //4. 设置map输出的k 和 v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5. 设置最终输出的k 和 v的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:/input/inputoutputformat"));
        FileOutputFormat.setOutputPath(job,new Path("D:/output"));

        //设置OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        job.setNumReduceTasks(2);
        //7. 提交job
        job.waitForCompletion(true);
    }
}
