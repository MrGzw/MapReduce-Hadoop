package com.atguigu.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * combiner组件 还是要继承 Reducer ， 但是Combiner是在MapTask运行的。
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable outV  = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //在每个MapTask中局部汇总数据
        int sum = 0 ;

        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        context.write(key,outV);
    }
}
