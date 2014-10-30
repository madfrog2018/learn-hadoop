package com.pxene.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URLEncoder;

import org.mortbay.util.UrlEncoded;


public class MakeData {
	
	public static void makeData(){
		
		try {
			
			File f = new File("D:\\git\\learn-hadoop\\resources\\a.txt");
			BufferedWriter bf = null;
			if (f.exists()) {
				System.out.println("file exists");
			}else{
				
				if (f.createNewFile()) {
					System.out.println("create file");
				}else {
					System.out.println("create file error");
				}
				String str = null;
				bf = new BufferedWriter(new FileWriter(f));
				FileInputStream in = new FileInputStream(new File("D:\\git\\learn-hadoop\\resources\\words.txt"));
				InputStreamReader ins = new InputStreamReader(in);
				BufferedReader br = new BufferedReader(ins);
				int line = 1;
				while ((str = br.readLine()) != null) {
					
					Data data = null;
					String urlEncoder = URLEncoder.encode(str, "UTF-8");
					if (line <= 25) {
						data = new Data(String.valueOf(line), "http://m.baidu.com/from=844b/pu=sz%401320_2001/s?"
								+ "word="+urlEncoder+"&ts=4365185&t_kt=292&sa=ih_4&ss=010");
					}else if (line > 25 && line <= 50) {
						
						data = new Data(String.valueOf(line), "http://m.sm.cn/s?q=" +urlEncoder+ "&from=sm&by=submit&snum=6");
					}else if (line > 50 && line <=100 ) {
						data = new Data(String.valueOf(line), "http://m.so.com/s?q=" +urlEncoder+ "&src=suggest&srcg=home"
								+ "&pq=%E7%99%BD%E9%85%92&_ms=0&log_id=6693647");
					}else {
						data = new Data(String.valueOf(line), "http://wap.sogou.com/web/searchList.jsp?uID=ZdMUhqAyEI0anlIN&v=5&w=1274&"
								+ "t=1414637066806&s_t=1414637069911&keyword="+urlEncoder+"&pg=webSearchList");
					}
					
					bf.write(data.getVisitTime());
					bf.write(0x01);
					bf.write(data.getImsi());
					bf.write(0x01);
					bf.write(data.getMdn());
					bf.write(0x01);
					bf.write(data.getUrl());
					bf.newLine();
					bf.flush();
					line++;
				}
				bf.close();
			}
		} catch (Exception e) {
			
		} finally{
			//
		}
	}

	public static void main(String[] args) {
		
		makeData();
	}
}
