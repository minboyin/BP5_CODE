package com.minboyin.Combiner;

import com.minboyin.flow.sum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/27 23:45
 *  * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *  *     KEYIN: map阶段传递过来的key的类型
 *  *     VALUEIN:map阶段传递过来的value的类型
 *  *     KEYOUT: 局部汇总的key的类型，也就是单词的类型
 *  *     VALUEOUT: 局部汇总的value的类型，也就是次数的类型
 */

public class MyCombiner extends Reducer<Text, FlowBean,Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1、初始化局部变量
        Integer upFlow=0;
        Integer downFlow=0;
        Integer upCountFlow=0;
        Integer downCountFlow=0;

        //2、获取数据并实现业务逻辑：同一个手机号码下面的四个数据，各自求和
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow +=value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        //3、将结果封装到FlowBean
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        //4、写入上下文
        context.write(key,flowBean);
    }
}
