package com.atguigu.mr.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OutputFormatReducer  extends Reducer<Text, NullWritable,Text,NullWritable> {

    //关于 key  的问题后面会聊
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //迭代values，直接写出
        for (NullWritable value : values) {
            context.write(key,value);
        }
    }
}
