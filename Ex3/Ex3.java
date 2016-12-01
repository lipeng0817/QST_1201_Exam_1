package qst.lps;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Ex3 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private HashSet<String> set =new HashSet<String>();

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			FileSystem fs =FileSystem.get(conf);
			Path path=new Path(conf.get("ip_path"));
			InputStream in=fs.open(path);
			Scanner sc=new Scanner(in);
			
			while(sc.hasNext()){
				String[] s=sc.nextLine().split("\t");
				set.add(s[0]);
			}
		}
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split =value.toString().split("\t");
			if(set.contains(split[0])){
				context.write(new Text(split[0]), new Text("1"));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private int num=0;
		
		protected void reduce(Text key, Iterable<Text> values, Context arg2)
				throws IOException, InterruptedException {
			num++;
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			context.write(new Text("num"), new Text(String.valueOf(num)));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf =new Configuration();
	
		conf.set("ip_path", args[2]);
		Job job=Job.getInstance(conf,"" + Ex3.class);
		job.setJarByClass(Ex3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		return;
	}
}
