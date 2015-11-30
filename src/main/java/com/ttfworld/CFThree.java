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

public class CFThree {
	public static class Map extends Mapper<Object, Text,  IntWritable,Text> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			context.write(one,new Text(line[0]));
		}


	}

	public static class Reduce extends
			Reducer<IntWritable, Text, Text, Text> {
		private final static int N=1000;

		public void reduce(IntWritable  key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int i=0;
			String CT[];
		    CT=new String[N];
			for (Text val : values) {
				String Two[]=val.toString().split("~");
				String path=Two[0]+"~"+Two[1];
				CT[i]=path;
				i++;
			}
			for(int j=0;j<i;j++){
				String can[]=CT[j].split("~");
				for(int m=0;m<i;m++){
					String ca[]=CT[m].split("~");
					if(can[1].equals(ca[0])){
						String Three=can[0]+"~"+can[1]+"~"+ca[1];
						context.write(new Text(Three), new Text("c"));
					}
				}
			}
		}
	}

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "CFThree");
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setJarByClass(CFThree.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();
		String input = "hdfs://localhost:9000/jiaodian1/Frequent/Two";
		String output = "hdfs://localhost:9000/jiaodian1/Candidate/Three";

		int exitCode = CFThree.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}

}
