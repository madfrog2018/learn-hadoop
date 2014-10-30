package com.pxene.hadoop;

public class Data {

	private String number;
	private String url;
	private String visitTime = "20140915095955";
	private String imsi;
	private String mdn = "74a309a31c381210f1ba6f4d76d9a6e1";
	
	
	public Data(String number, String url) {
		this.number = number;
		this.url = url;
	}


	public String getNumber() {
		return number;
	}


	public void setNumber(String number) {
		this.number = number;
	}


	public String getUrl() {
		return url;
	}


	public void setUrl(String url) {
		this.url = url;
	}


	public String getVisitTime() {
		return visitTime;
	}


	public void setVisitTime(String visitTime) {
		this.visitTime = visitTime;
	}


	public String getImsi() {
		System.out.println(number);
		return imsi = "d9b941ea39e559a8"+ number +"c3f8aa037410";
	}


	public void setImsi(String imsi) {
		this.imsi = imsi;
	}


	public String getMdn() {
		return mdn;
	}


	public void setMdn(String mdn) {
		this.mdn = mdn;
	}

}
