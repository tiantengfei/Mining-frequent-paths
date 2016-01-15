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

        String sessionID = logArray[4];
        String visitTime = logArray[3];
        String refID = logArray[8];
        String reqID= logArray[9];


            // value：tme～referer～request
           valueOut.set(visitTime + "~" + refID + "~" + reqID);
            context.write(new Text(sessionID), valueOut);
        }

    }// map


