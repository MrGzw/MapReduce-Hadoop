package com.atguigu.mr.diyPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器， 继承 Partitioner类
 */
public class PhoneNumPartitioner extends Partitioner<Text,FlowBean> {

    /**
     * 需求:  136 137  138  139  其他
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partitioner ;
        //将key处理成String
        String key = text.toString();
        if(key.startsWith("136")){
            partitioner =0 ;
        }else if (key.startsWith("137")){
            partitioner =1 ;
        }else if (key.startsWith("138")){
            partitioner =2 ;
        }else if (key.startsWith("139")){
            partitioner =3 ;
        }else{
            partitioner =4 ;
        }

        return partitioner;
    }
}
