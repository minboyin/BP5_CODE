package com.minboyin.flow.sum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by minbo on 2021/1/24 18:11
 ** 流量FlowBean的实体类
 *     | 上行数据包 | upFlow        | int    |
 *     | 下行数据包 | downFlow      | int    |
 *     | 上行流量   | upCountFlow   | int    |
 *     | 下行流量   | downCountFlow | int    |
 */

public class FlowBean implements WritableComparable<FlowBean> {
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
    //MapperReducer只能对key进行排序，此处虽然有比较的代码，但是由于FlowBean为value，所以不起作用
    //根据return结果的正负，确定当前对象（this）和比较对象（o）的位置关系；正数,当前对象在后，升序；负数，当前对象在前，倒序
    //compareTo:如果指定的数与参数相等返回0;如果指定的数小于参数返回-1;如果指定的数大于参数返回1

    @Override
    public int compareTo(FlowBean o) {

        return o.upFlow - this.upFlow;
        //return this.upFlow > o.upFlow ? -1 :1;
        //方式二：return this.upFlow.compareTo(o.upFlow) * -1
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
