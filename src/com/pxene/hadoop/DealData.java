package com.pxene.hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DealData {

	
	public static class DataMapper extends Mapper<Text, Text, Text, Text>{
		
		
		public void map(Text key, Text value, Context context)throws IOException, InterruptedException{
			
			String[] lines = value.toString().split("\t\n");
			for (int i = 0; i < lines.length; i++) {
				Pattern pattern = Pattern.compile("*m.baidu.com*|*m.sm.cn*|*m.sougou.com*|*m.so.com*");
				Matcher matcher = pattern.matcher(lines[i]);
				System.out.println("line data is " + lines[i]);
				if (matcher.find()) {
					
					String[] rows = lines[i].split("\0x01");
					context.write(new Text(rows[1]), new Text(rows[3]));
					//TODO rows[3] get the abstract word
					System.out.println("IMSI is " + rows[1]);
					System.out.println("IMSI is " + rows[3]);
				}
			}
		}
	}
	
	public static class CombineReduce extends Reducer<Text, Text, Text, Text>{
		
		private String str = "";
	    public void reduce(Text key, Iterable<Text> values, 
                Context context) throws IOException, InterruptedException{
	    	
	    	for (Text text : values) {
				
	    		str += text.toString();
			}
	    	
	    	Text result = new Text(str);
	    	context.write(key, result);
	    }
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: dealData <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "DealData");
	    job.setJarByClass(DealData.class);
	    job.setMapperClass(DataMapper.class);
	    job.setCombinerClass(CombineReduce.class);
	    job.setReducerClass(CombineReduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job,
	      new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
