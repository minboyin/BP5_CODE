package com.miniboyin.分组组件;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.WritableComparable;
import org.omg.CORBA_2_3.portable.OutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by minbo on 2021/1/26 20:21
 */

public class OrderBean  implements WritableComparable{
    private String priceId;
    private Double price;

    public String getPriceId() {
        return priceId;
    }

    public void setPriceId(String priceId) {
        this.priceId = priceId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public String toString() {
        return "com.miniboyin.分组组件.OrderBean{" +
                "priceId='" + priceId + '\'' +
                ", price=" + price +
                '}';
    }
}
