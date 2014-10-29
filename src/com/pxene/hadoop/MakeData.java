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
			if (f.exists()) {
				System.out.println("file exists");
			}else{
				
				if (f.createNewFile()) {
					System.out.println("create file");
				}else {
					System.out.println("create file error");
				}
				String str = null;
				BufferedWriter bf = new BufferedWriter(new FileWriter(f));
				FileInputStream in = new FileInputStream(new File("D:\\git\\learn-hadoop\\resources\\a.txt"));
				InputStreamReader ins = new InputStreamReader(in);
				BufferedReader br = new BufferedReader(ins);
				int line = 1;
				while ((str = br.readLine()) != null) {
					
					String urlEncoder = URLEncoder.encode(str, "UTF-8");
					new Data(String.valueOf(line), "");
					
				}
			}
		} catch (Exception e) {
			
			
		}

		
	}

}
