package com.atguigu.mr.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    private FlowBean outK = new FlowBean();  // hashcode
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     //处理一行数据
     //   1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();
        String[] splits = line.split("\t");
     // 封装
        outV.set(splits[1]);

        outK.setUpFlow(Long.parseLong(splits[splits.length-3]));
        outK.setDownFlow(Long.parseLong(splits[splits.length-2]));
        outK.setSumFlow();

     //写出
        context.write(outK,outV);
    }
}
