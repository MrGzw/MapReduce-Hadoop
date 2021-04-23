package com.atguigu.mr.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;

/**
 * 自定义RecordWriter，需要继承 RecordWriter类，并重写write和close方法.
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {


    private String atguiguPath = "d:/output/atguigu.log";
    private String otherPath = "d:/output/other.log";
    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;
    private TaskAttemptContext context ;

    public LogRecordWriter(TaskAttemptContext context) throws IOException {
        this.context = context;
    }

    /**
     * 需求: 包含atguigu，写到d:/output/atguigu.log
     *      其他的数据写到 d:/output/other.log
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        //获取文件系统对象
        FileSystem fs = FileSystem.get(context.getConfiguration());

        //考虑多个分区，多个ReduceTask的场景,不同分区的数据写到不同的文件中，按照分区号决定。
        //计算当前key的分区号
        int partition = (key.hashCode() & Integer.MAX_VALUE) % context.getNumReduceTasks();
        //流为null则创建，流不为空，则直接使用
        if(atguiguOut ==null){
            atguiguOut = fs.create(new Path(atguiguPath+"."+partition));
        }
        if(otherOut ==null){
            otherOut = fs.create(new Path(otherPath+"."+partition));
        }
        //判断数据，使用不同的流将数据写出
        String log  = key.toString();
        if(log.contains("atguigu")){
            atguiguOut.writeBytes(log+"\n\r");
        }else {
            otherOut.writeBytes(log+"\n\r");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关闭流
        atguiguOut.close();
        otherOut.close();

    }
}
