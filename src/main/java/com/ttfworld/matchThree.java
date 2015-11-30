package com.ttfworld;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class matchThree {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String val = line[0] + "~" + line[1];
			String pages[] = line[0].split("~");
			// context.write(new Text(val), new Text("c"));
			if (pages.length >= 3) {
				String can[];//避免重复发送
				can = new String[pages.length];
				//int j = 0;
				for (int i = 0; i < pages.length - 2; i++) {
					String s = (String) pages[i];
					String r = (String) pages[i + 1];
					String t=(String)pages[i+2];
					int ret = s.compareTo(r);
					String min = "";
					if (ret <= 0) {
						int re=s.compareTo(t);
						if(re<=0){min=s;}
						else{min=t;}
						//min = s;
						// context.write(new Text(s), new Text(val));
					}
					if (ret > 0) {
						int rt=r.compareTo(t);
						if(rt<=0){min=r;}
						else{min=t;}
						//min = r;
						// context.write(new Text(r), new Text(val));
					}
					if (i == 0) {
						can[i] = min;
						context.write(new Text(min), new Text(val));
					} else {
						for (int k = 0; k < i; k++) {
							if (min.equals(can[k])) {
								break;
							}
							if (k == i - 1) {
								can[i] = min;
								context.write(new Text(min), new Text(val));
							}
						}
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
		private static int N = 10000;
		private IntWritable result = new IntWritable();
		//private final static IntWritable one = new IntWritable(1);

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int i = 0, j = 0;
			String c[], t[];
			c = new String[N];
			t = new String[N];
			for (Text val : values) {
				/*
				 * String k=key.toString(); String v=val.toString(); String
				 * ret=k+"#"+v; context.write(new Text(ret), one);
				 */
				String line[] = val.toString().split("~");
				String path = line[0];
				for (int k = 1; k < line.length - 1; k++) {
					path += "~" + line[k];
				}
				if (line[line.length - 1].equals("c")) {
					c[i] = path;
					// context.write(new Text(c[i]), one);
					i++;

				}
				if (line[line.length - 1].equals("t")) {
					t[j] = path;
					// context.write(new Text(t[j]), one);
					j++;
				}
			}
			for (int l = 0; l < i; l++) {
				String linec[] = c[l].split("~");// 候选
				int sum = 0;
				for (int m = 0; m < j; m++) {
					String linet[] = t[m].split("~");// 路径

					for (int n = 0; n < linet.length - 2; n++) {// 路径匹配
						if (linec[0].equals(linet[n])
								&& linec[1].equals(linet[n + 1])&&linec[2].equals(linet[n+2])) {
							sum += 1;
						}
					}

				}
				if (sum >= 5) {
					result.set(sum);
					context.write(new Text(c[l]), result);
				}
			}
		}
	}

	public static int run(String args[], String output) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "match");
		FileInputFormat.addInputPaths(job, args[0] + "," + args[1] );
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setJarByClass(matchThree.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();
		String input[];
		input=new String[2];
		input[0]= "hdfs://localhost:9000/jiaodian1/Two/path";
		input[1]= "hdfs://localhost:9000/jiaodian1/Candidate/Three";
		String output = "hdfs://localhost:9000/jiaodian1/Frequent/Three";

		int exitCode = matchThree.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}

}
