package com.atguigu.mr.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper  extends Mapper<LongWritable, Text,Text,OrderBean> {

    private String currentSplitFileName ;

    private OrderBean outV = new OrderBean() ;

    private Text outK = new Text() ;
    /**
     * 在MapTask执行开始时执行一次
     *
     * //获取当前处理的切片对应的文件是哪个
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       //获取当前的切片对象
        InputSplit inputSplit = context.getInputSplit();
       //转换成FileSplit
        FileSplit currentSplit = (FileSplit)inputSplit;
       //获取当前处理的文件名
        currentSplitFileName = currentSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理一行数据
        String line  = value.toString();
        String[] split = line.split("\t");
        if(currentSplitFileName.contains("order")){
            //数据来自于 order.txt
            //  1001	01	1
            //封装key
            outK.set(split[1]);
            //封装value
            outV.setOrderId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");

        }else{
            //数据来自于 pd.txt
            // 01	小米
            //封装key
            outK.set(split[0]);
            //封装value
            outV.setPid(split[0]);
            outV.setPname(split[1]);
            outV.setOrderId("");
            outV.setAmount(0);
            outV.setFlag("pd");
        }

        //写出
        context.write(outK,outV);
    }
}
