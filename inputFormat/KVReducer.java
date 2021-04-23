package com.atguigu.mr.inputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVReducer  extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable outV= new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0 ;
        for (IntWritable value : values) {
            sum += value.get();
        }

        //封装outV
        outV.set(sum);

        context.write(key,outV);

    }
}
