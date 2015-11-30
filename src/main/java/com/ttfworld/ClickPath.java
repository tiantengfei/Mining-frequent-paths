//package com.ttfworld;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
///**sesison~ip~country~device~browser相同的情况下，在一段时间内的点击路径和顺序
// * hdfs >>hdfs
//  日志中的相关字段处理
//1 	ip
//8 	request 	访问请求的url
//9 	referer 	访问来自的url
//10 	status 		请求状态(200)
//14 	fourdomain 	网站（站的域名：made-in-china.com）
//15 	spider_number 	爬虫标记，如果不是-1则是网络爬虫
//36 	country_name 	国家名称，取不到，国家为-
//38 	session         取不到的为-1
//41 	normal_flag 	异常访问标记
// *
// * 输出到hdfs，
// * key:sesison~ip~country~device~browser~startTime~endTime
// * value:time1~url1~url2~tim2~url2~url2
// * 用于lognext中的进一步计算其他指标
// *
// */
//
//
//	public static class Map extends Mapper<Object, Text, Text, Text> {
//		private Text keyOut = new Text();
//		private Text valueOut = new Text();
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//
//			// 对一行日志记录 拆分
//			String[] logArray = value.toString().split("\t");
//			// 对request,去掉GET等
//			String[] requestStr = logArray[8].split(" ");
//			// 限定条件
//			if (logArray[41].equals("normal") && logArray[15].equals("-1")
//					&& requestStr[0].equals("GET")
//					&& logArray[10].equals("200")
//					&& (!logArray[38].equals("-1"))
//					) {
//
//               //对logArray[12]处理，得到device和browser
//				String device="-1";
//				//浏览器不好弄
//				String browser="-1";
//				if(!logArray[12].equals("-")){
//					if (logArray[12].contains("Mobile")){
//						device="mobile";
//					}else if(logArray[12].contains("Tablet")){
//						device="tablet";
//					}else{
//						device="pc";
//					}
//					if (logArray[12].contains("Safari")){
//						browser="Safari";
//					}else if(logArray[12].contains("Chrome")){
//						browser="Chrome";
//					}else if(logArray[12].contains("Firefox")){
//						browser="Firefox";
//					}else  if(logArray[12].contains("Opera")){
//						browser="Opera";
//					}else{
//						browser="IE"; //默认都是IE
//					}
//				}
//
//				// key: sesison~ip~country~device~browser
//				keyOut.set(logArray[38] + "~" + logArray[1] + "~"
//						+ logArray[36]+"~"+device+"~"+browser);
//
//				// 去掉referer中的 http:// 和 https:// 和 HTTP：//
//				String referer = logArray[9].replace("http://", "")
//						.replace("https://", "").replace("HTTP://", "");
//
//				// request(fourdomain+)
//				String request="";
//				if(logArray[14].equals("-1")){  //fourdomain="-1"sesison~ip~country~device~browser
//				 request ="made-in-china.com"
//						+ requestStr[1];
//				}else{
//			     request = logArray[14] + "made-in-china.com"
//							+ requestStr[1];
//				}
//				// value：时间～referer～request
//				valueOut.set(logArray[4] + "~" + referer + "~" + request);
//
//				context.write(keyOut, valueOut);
//
//			}
//
//		}// map
//	}// Map
////key: sesison~ip~country~device~browser  value：时间～referer～request
//	 public class Reduce extends Reducer<Text, Text, NullWritable, Text> {
//		public void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//			// 存储会话开始时间
//			String startTime = null;
//			String endTime = null;
//
//			ArrayList<String> tmpEdgeList = new ArrayList<String>();
//			// 将一个key的所用路段 都存储起来，接下来按时间排序
//			for (Text text : values) {
//				tmpEdgeList.add(text.toString());
//			}
//			Object[] sortedEdgeList = tmpEdgeList.toArray();
//			Arrays.sort(sortedEdgeList);
////session 时间
//			startTime = ((String) sortedEdgeList[0]).split("~")[0];
//			endTime = ((String) sortedEdgeList[sortedEdgeList.length - 1])
//					.split("~")[0];
//
//			String path="";
//			//path存放的是t1~a~b~t2~b~c~
//			for (int i = 0; i < sortedEdgeList.length; i++) {
//				path  += ((String) sortedEdgeList[i])+"~";
//
//			}
//			//key: sesison~ip~country~device~browser~startTime~endTime
//			//value: time~url~url
//			context.write(null, new Text(path));
//
//		}// reduce
//	}// Reduce
////
////	@SuppressWarnings("deprecation")
////	public static int run(String input, String output) throws IOException,
////			ClassNotFoundException, InterruptedException {
////
////		Configuration conf = new Configuration();
////
////		Job job = new Job(conf, "ClickPath");
////		FileInputFormat.addInputPath(job, new Path(input));
////		FileOutputFormat.setOutputPath(job, new Path(output));
////
////		job.setJarByClass(ClickPath.class);
////		job.setMapperClass(Map.class);
////		job.setReducerClass(Reduce.class);
////
////		job.setMapOutputKeyClass(Text.class);
////		job.setMapOutputValueClass(Text.class);
////
////		job.setOutputKeyClass(Text.class);
////		job.setOutputValueClass(Text.class);
////
////		job.setInputFormatClass(TextInputFormat.class);
////		job.setOutputFormatClass(TextOutputFormat.class);
////
////		return job.waitForCompletion(true) ? 0 : 1;
////
////	}
//
//
