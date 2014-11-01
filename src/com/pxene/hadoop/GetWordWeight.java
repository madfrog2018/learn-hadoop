package com.pxene.hadoop;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetWordWeight {

	
	public static class GetWordWeightMapper extends Mapper<Text, Text, MapWritable, Text>{
		
		public void map(Text key, Text value, Context context){
			
			MapWritable map = new MapWritable();
			String[] values = value.toString().split(",");
			for (String string : values) {
				
				map.put(key, new Text(string));
			}
		}
	}
}
