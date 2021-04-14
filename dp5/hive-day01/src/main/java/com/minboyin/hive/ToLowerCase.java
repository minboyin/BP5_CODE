package com.minboyin.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by minbo on 2021/2/25 15:37
 */

public class ToLowerCase extends UDF {
    public String evaluate(String field)
    {
        String result = field.toLowerCase();
        return result;
    }
}
