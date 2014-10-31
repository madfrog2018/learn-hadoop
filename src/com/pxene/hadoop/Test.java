package com.pxene.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.Query;

import org.apache.commons.httpclient.HttpsURL;




public class Test {

	public static void main(String[] args) {
		
//		File file = new File("D:\\git\\learn-hadoop\\resources\\Test.txt");
//		Pattern pattern = Pattern.compile(".*m.baidu.com.*|.*m.sm.cn.*|.*m.sougou.com.*|.*m.so.com.*");
//		char c = 0x01;
////		System.out.println(c);
//		Character character = new Character(c);
//		System.out.println(character.toString());
//		
//		try {
//			String string = null;
//			FileInputStream inputStream = new FileInputStream(file);
//			InputStreamReader reader = new InputStreamReader(inputStream);
//			BufferedReader bReader = new BufferedReader(reader);
//			int line = 1;
//			while((string = bReader.readLine()) != null){
//				System.out.println(string);
//				String[] strs = string.split(character.toString());
////				Matcher matcher = pattern.matcher(bReader.readLine());
////				if (matcher.find()) {
////					String[] rows = bReader.readLine().split("\0x01");
////					System.out.println(rows[1]);
////					System.out.println(rows[3]);
////				}
//				
//				System.out.println("line --------------------------------" + line);
//				line++;
//			}
//			bReader.close();
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		String str = "2014091509595509b7c5247eaf11676d478303f6c5d40a94040c1bb5668c21da6367db9f0400d3m.baidu.com/api/news/v4/article.go?"
//				+ "rt=xml&newsId=23721657&channelId=2&supportTV=1&imgTag=1&recommendNum=3&showSdkAd=1&openType=0&subId=248&from=channel&net=WWAN&p1=NTc1NzMwMzYxMDY0Mzg0NTEyMg==&pid=-1&apiVersion=22&sid=10&u=1101.227.172.248010.14.84.18449368sohu.com  4";
//		Matcher matcher = pattern.matcher(str);
//		boolean b = matcher.matches();
//		System.out.println(b);
//		String[] strings = str.split(new Character((char) 0x01).toString());
//		System.out.println(strings[1]);
//		System.out.println("---------");
//		System.out.println(strings[3]);
		
		
		String string = "%E8%AE%A1%E7%AE%97%E6%9C%BA";
		String wordString = null;;
		try {
			wordString = URLDecoder.decode(string, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(wordString);
		
		
		
//		Pattern pattern = Pattern.compile(".*m.baidu.com.*|.*m.sm.cn.*|.*m.sougou.com.*|.*m.so.com.*");
//		String str = "12m.baidu.com";
//		Matcher matcher = pattern.matcher(str);
//		System.out.println(matcher.matches());
		
		
		Pattern pattern = Pattern.compile(".*?word=.*");
		
		String string2 = "http://wap.sogou.com/web/searchList.jsp?uID=ZdMUhqAyEI0anlIN&v=5&"
				+ "w=1274&t=1414637066806&s_t=1414637069911&keyword=%E7%BE%8E%E5%9B%BD&pg=webSearchList";
		
		Matcher matcher = pattern.matcher(string2);
		System.out.println(matcher.matches());
		int start = string2.indexOf("keyword=");
		System.out.println(start);
		int end = string2.indexOf("&", start);
		System.out.println(end);
		String key = string2.substring(start + 8, end);
		System.out.println(key);
		
		try {
			URL url = new URL(string2);
			String query = url.getQuery();
			System.out.println(query);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
}
