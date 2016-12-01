package qst.lps;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.Reducer;

public class Ex2 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Set<String> set = new HashSet<String>();

		protected void map(LongWritable key, Text value, Context context) {
			Configuration conf = context.getConfiguration();
			String line = value.toString();
			String pattern = "(\\d+.\\d+.\\d+.\\d+)(.+)";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(value.toString());
			if (m.find()) {
				set.add(m.group(1));
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String m = set.size() + "";
			context.write(new Text("num"), new Text(m));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Ex2.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		return;
	}

}
