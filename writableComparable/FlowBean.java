package com.atguigu.mr.writableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowBean implements WritableComparable<FlowBean> {

    private Long upFlow;

    private Long downFlow ;

    private Long sumFlow ;

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public String toString() {
       return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }



    public FlowBean(){}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    /**
     * 比较规则:  按照总流量倒序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        return -this.sumFlow.compareTo(o.getSumFlow());
    }

    /*
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowBean flowBean = (FlowBean) o;
        return Objects.equals(upFlow, flowBean.upFlow) &&
                Objects.equals(downFlow, flowBean.downFlow) &&
                Objects.equals(sumFlow, flowBean.sumFlow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(upFlow, downFlow, sumFlow);
    }

    */
}
