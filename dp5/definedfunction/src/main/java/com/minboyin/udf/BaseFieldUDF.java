package com.minboyin.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by minbo on 2021/3/17 13:30
 * /**
 *  *  * 步骤：
 *  *  （1）将传入的key用逗号 "，" 号切割：
 *  *  String[] jsonkeys = mid uid vc vn l sr os ar md ba sv g hw nw ln la t
 *  *  （2）将传入的line，用“|”切割：取出服务器时间serverTime和json数据
 *  *  （3）根据切割后获取的json数据，创建一个JSONObject对象
 *  *  （4）根据创建的JSONObject对象，传入key值"cm"得到公共字段的json对象cmJson
 *  *  （5）循环遍历jsonkeys，根据key值，获取cmJson中的value，将所有value值通过\t拼接在一起
 *  *
 */

public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String jsonkeysString){

        //1、创建一个 StringBuilder对象实例
        StringBuilder builder = new StringBuilder();

        //2、将参数 jsonkeysString 中的数据存储在 String[] jsonkeys
        String[] jsonkeys = jsonkeysString.split(",");

        //3、将参数line的数据解析，并存储在 String[] logContents
        String[] logContents = line.split("\\|");

        //4、合法性校验：解析后数据的长度 || 解析后需要分析的数据是否为 null
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])){
            return "";
        }

        //5、builder 添加数据：一、循环遍历 cm 中的数据并添加；二、将 en 中的数据全部存储；三、添加 line 解析到 logContents 的首个数据
        try {
            // 获取 logContents 中的第一个数据，即 JSONObject 对象实例
            JSONObject jsonObject = new JSONObject(logContents[1]);

            // 获取 "cm" 的 json 对象
            JSONObject base = jsonObject.getJSONObject("cm");

            //循环遍历，根据 jsonkeys ，获取  "cm" 的 json 对象 中的数据
            for (int i =0 ;i<jsonkeys.length;i++){
                String filedName = jsonkeys[i].trim();

                // 数据存储
                if (base.has(filedName)){
                    builder.append(base.getString(filedName)).append("\t");
                }else {
                    builder.append("\t");
                }

            }
            //数据存储
            builder.append(jsonObject.getString("et")).append("\t");
            builder.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        //6、返回结果数据：builder
        return builder.toString();
    }

    public static void main(String[] args) {
        String line = "1598693683508|{\"cm\":{\"ln\":\"-108.1\",\"sv\":\"V2.7.4\",\"os\":\"8.0.4\",\"g\":\"6G07BL4X@gmail.com\",\"mid\":\"3\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"11\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"3\",\"t\":\"1598627949192\",\"la\":\"5.0\",\"md\":\"sumsung-0\",\"vn\":\"1.0.0\",\"ba\":\"Sumsung\",\"sr\":\"X\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1598647856166\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"2\",\"goodsid\":\"0\",\"news_staytime\":\"16\",\"loading_time\":\"9\",\"action\":\"3\",\"showtype\":\"1\",\"category\":\"73\",\"type1\":\"201\"}},{\"ett\":\"1598654400505\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"12\",\"action\":\"1\",\"extend1\":\"\",\"type\":\"2\",\"type1\":\"\",\"loading_way\":\"1\"}},{\"ett\":\"1598603902380\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1598632379633\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\"}},{\"ett\":\"1598686251553\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1598693035607\",\"praise_count\":283,\"other_id\":9,\"comment_id\":4,\"reply_count\":8,\"userid\":7,\"content\":\"焉蔫分偏挽拄久蝇寝堤蛇脸钉潘医都\"}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }
}
