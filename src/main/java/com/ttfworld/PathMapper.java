package com.ttfworld;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by ttf on 15-11-29.
 */
public class PathMapper extends Mapper<Object, Text, Text, Text>{

    private Text keyOut = new Text();
    private Text valueOut = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // 对一行日志记录 拆分
        String[] logArray = value.toString().split("\t");
        // 对request,去掉GET等
        String[] requestStr = logArray[8].split(" ");
        // 限定条件
        if (logArray[41].equals("normal") && logArray[15].equals("-1")
                && requestStr[0].equals("GET")
                && logArray[10].equals("200")
                && (!logArray[38].equals("-1"))
                ) {

            //对logArray[12]处理，得到device和browser
            String device="-1";
            //浏览器不好弄
            String browser="-1";
            if(!logArray[12].equals("-")){
                if (logArray[12].contains("Mobile")){
                    device="mobile";
                }else if(logArray[12].contains("Tablet")){
                    device="tablet";
                }else{
                    device="pc";
                }
                if (logArray[12].contains("Safari")){
                    browser="Safari";
                }else if(logArray[12].contains("Chrome")){
                    browser="Chrome";
                }else if(logArray[12].contains("Firefox")){
                    browser="Firefox";
                }else  if(logArray[12].contains("Opera")){
                    browser="Opera";
                }else{
                    browser="IE"; //默认都是IE
                }
            }

            // key: sesison~ip~country~device~browser
            keyOut.set(logArray[38] + "~" + logArray[1] + "~"
                    + logArray[36]+"~"+device+"~"+browser);

            // 去掉referer中的 http:// 和 https:// 和 HTTP：//
            String referer = logArray[9].replace("http://", "")
                    .replace("https://", "").replace("HTTP://", "");

            // request(fourdomain+)
            String request="";
            if(logArray[14].equals("-1")){  //fourdomain="-1"sesison~ip~country~device~browser
                request ="made-in-china.com"
                        + requestStr[1];
            }else{
                request = logArray[14] + "made-in-china.com"
                        + requestStr[1];
            }
            // value：时间～referer～request
           // valueOut.set(logArray[4] + "~" + referer + "~" + request);
            System.out.println((logArray[4] + "~" + request));
           // context.write(keyOut, new Text(logArray[4] + "~" + request));
            context.write(new Text(logArray[38] + "-----hello"), new Text(logArray[4] + "~" + request));
        }

    }// map
}// Map

