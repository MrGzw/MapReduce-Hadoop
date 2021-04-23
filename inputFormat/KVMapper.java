package com.atguigu.mr.inputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class KVMapper  extends Mapper<Text,Text,Text, IntWritable> {
    IntWritable outV = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //读取到的一行数据会按照预先设置好的分隔符切开， 切开以后 左边的作为key ,右边的作为value 进入到Mapper中
        //例如:  banzhang ni hao , 设置的分隔符为" " ,  切开以后: K ： banzhang  V: ni hao

        context.write(key,outV);
    }
}
