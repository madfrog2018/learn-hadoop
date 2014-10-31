package com.pxene.hadoop;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class NewGetWords {

	public static class NewGetWordsMapper 
    	extends Mapper<Object, Text, Text, Text>{
 
	   
		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {

				
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
				if (bdMatcher.matches() || smMatcher.matches() || 
						sougouMatcher.matches() || soMatcher.matches()) {
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
								System.out.println("baidu keywords is " + URLDecoder.decode(keyWords, "UTF-8"));
								context.write(new Text(rows[1]), new Text(URLDecoder.decode(keyWords, "UTF-8")));
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
								System.out.println("sm and so keywords is " + URLDecoder.decode(keyWords, "UTF-8"));
								context.write(new Text(rows[1]), new Text(URLDecoder.decode(keyWords, "UTF-8")));
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
							System.out.println("sougou keywords is " + URLDecoder.decode(keyWords, "UTF-8"));
							context.write(new Text(rows[1]), new Text(URLDecoder.decode(keyWords, "UTF-8")));
						}
					}
					//TODO rows[3] get the abstract word
					System.out.println("IMSI is " + rows[1]);
					System.out.println("url is " + rows[3]);
				}
			
			 
		 }
	}
	
	public static class NewGetWordsReducer extends Reducer<Text,Text,Text,Text> {
	
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			boolean first = true;
		    StringBuilder toReturn = new StringBuilder();
		    for (Text text : values) {
		    	if (!first){
		    		toReturn.append(",");
		    	}
		        first=false;
		        toReturn.append(text.toString());
		    }

		    context.write(key, new Text(toReturn.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		 Configuration conf = new Configuration();
		 String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 if (remainingArgs.length < 2) {
		   System.err.println("Usage: wordcount <in> [<in>...] <out>");
		   System.exit(2);
		 }
		 Job job = Job.getInstance(conf, "New GetWords");
		 job.setJarByClass(NewGetWords.class);
		 job.setMapperClass(NewGetWordsMapper.class);
		 job.setCombinerClass(NewGetWordsReducer.class);
		 job.setReducerClass(NewGetWordsReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 List<String> otherArgs = new ArrayList<String>();
		 for (int i=0; i < remainingArgs.length; ++i) {
			 if ("-skip".equals(remainingArgs[i])) {
				 job.addCacheFile(new Path(remainingArgs[++i]).toUri());
				 job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			 } else {
				 otherArgs.add(remainingArgs[i]);
		     }
		 }
		 FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
