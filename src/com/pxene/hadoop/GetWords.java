package com.pxene.hadoop;

import java.io.IOException;
import java.net.URLDecoder;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class GetWords {
	
	public static class GetWordsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
				
			//value is the data of line
			String keyWords = null;
			Pattern bdPattern = Pattern.compile(".*m.baidu.com.*");
			Pattern smPattern = Pattern.compile(".*m.sm.cn.*");
			Pattern sougouPattern = Pattern.compile(".*m.sougou.com.*");
			Pattern soPattern = Pattern.compile(".*m.so.com.*");
			
			Matcher bdMatcher = bdPattern.matcher(value.toString());
			Matcher smMatcher = smPattern.matcher(value.toString());
			Matcher sougouMatcher = sougouPattern.matcher(value.toString());
			Matcher soMatcher = soPattern.matcher(value.toString());
			System.out.println("line data is " + value.toString());
			String[] rows = value.toString().split(new Character((char) 0x01).toString());
			String url = rows[3];
			if (bdMatcher.matches()) {
				//baidu search url
				Pattern wordPattern = Pattern.compile(".*?word=.*");
				Matcher wordMatcher = wordPattern.matcher(url);
				if (wordMatcher.matches()) {
					//include keywords
					int start = url.indexOf("?word=");
					int end = url.indexOf("&");
					if (start > 0 && end > 0) {
						keyWords = url.substring(start + 6, end);
					}
				}
			}
			
			if (smMatcher.matches() || soMatcher.matches()) {
				
				//shenma search && 360so search
				Pattern wordPattern = Pattern.compile(".*?q=.*");
				Matcher wordMatcher = wordPattern.matcher(url);
				if (wordMatcher.matches()) {
					
					//
					int start = url.indexOf("?q=");
					int end = url.indexOf("&");
					if (start > 0 && end > 0) {
						
						keyWords = url.substring(start + 3, end);
					}
				}
			}
			
			if (sougouMatcher.matches()) {
				
				//360so
				Pattern wordPattern = Pattern.compile(".*keyword=.*");
				Matcher wordMatcher = wordPattern.matcher(url);
				if(wordMatcher.matches()){
					
					int start = url.indexOf("keyword=");
					int end = url.indexOf("&", start);
					if (start > 0 && end > 0) {
						keyWords = url.substring(start + 8, end);
					}else {
						keyWords = url.substring(start);
					}
				}
			}
			
			output.collect(new Text(rows[1]), new Text(URLDecoder.decode(keyWords, "UTF-8")));
			//TODO rows[3] get the abstract word
			System.out.println("IMSI is " + rows[1]);
			System.out.println("url is " + rows[3]);
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
	public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(GetWords.class);
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: dealData <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    conf.setJobName("GetWords");
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(conf, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(conf,
		      new Path(otherArgs[otherArgs.length - 1]));
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
