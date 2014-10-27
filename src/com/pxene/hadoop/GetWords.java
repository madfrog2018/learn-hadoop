package com.pxene.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GetWords {
	
	public static class GetWordsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] lines = value.toString().split("\t\n");
			for (int i = 0; i < lines.length; i++) {
				Pattern pattern = Pattern.compile("*m.baidu.com*|*m.sm.cn*|*m.sougou.com*|*m.so.com*");
				Matcher matcher = pattern.matcher(lines[i]);
				System.out.println("line data is " + lines[i]);
				if (matcher.find()) {
					
					String[] rows = lines[i].split("\0x01");
					output.collect(new Text(rows[1]), new Text(rows[3]));
					//TODO rows[3] get the abstract word
					System.out.println("IMSI is " + rows[1]);
					System.out.println("url is " + rows[3]);
				}
			}
		}
	}
	
	public static class GetWordsReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			
			boolean first = true;
		    StringBuilder toReturn = new StringBuilder();
		    while (values.hasNext()){
		    	if (!first){
		    		toReturn.append(", ");
		    	}
		        first=false;
		        toReturn.append(values.next().toString());
		    }

		      output.collect(key, new Text(toReturn.toString()));
		}
	}
	public static void main(String[] args) {
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(GetWords.class);

	    conf.setJobName("GetWords");

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(conf, new Path("input"));
	    FileOutputFormat.setOutputPath(conf, new Path("output"));

	    conf.setMapperClass(GetWordsMapper.class);
	    conf.setReducerClass(GetWordsReducer.class);
	    client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }
}
