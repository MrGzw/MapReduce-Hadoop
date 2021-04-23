package com.atguigu.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job  = Job.getInstance(conf);
        //设置缓存文件
        job.addCacheFile(new URI("file:///D:/input/inputcache/pd.txt"));
        // job.addCacheFile();  可以设置多个缓存文件
        //.....
        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置ReduceTask的个数为0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("d:/input/inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output3"));
        job.waitForCompletion(true);
    }
}
