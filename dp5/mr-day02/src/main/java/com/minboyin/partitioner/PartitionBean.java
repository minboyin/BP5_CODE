package com.minboyin.partitioner;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by minbo on 2021/1/29 19:07
 */

public class PartitionBean implements WritableComparable<PartitionBean> {
    private Integer upFlow;
    private Integer downFlow;
    private Integer upCountFlow;
    private Integer downCountFlow;

    //1、重写set、get方法,快捷建：alt+insert

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    //2、重写toString方法，设定业务需求的输出格式数据
    @Override
    public String toString() {
        return  upFlow +
                "\t" + downFlow +
                "\t" + upCountFlow +
                "\t" + downCountFlow ;
    }

    //此需求无需排序，直接将源数据分区
    @Override
    public int compareTo(PartitionBean o) {
        return 0;
    }

    //3、由于需要进行数据传输，所以需要将对象序列化和反序列化

    /**
     *实现序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);
    }

    /**
     * 实现反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow=dataInput.readInt();
        this.downFlow=dataInput.readInt();
        this.upCountFlow=dataInput.readInt();
        this.downCountFlow=dataInput.readInt();
    }
}
