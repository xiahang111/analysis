package cn.tedu.storm.flux.dao;

import java.sql.Timestamp;

public class TongJi3Info {
	private Timestamp time;
	private double br;
	private double avgtime;
	private double avgdeep;
	public Timestamp getTime() {
		return time;
	}
	public void setTime(Timestamp time) {
		this.time = time;
	}
	public double getBr() {
		return br;
	}
	public void setBr(double br) {
		this.br = br;
	}
	public double getAvgtime() {
		return avgtime;
	}
	public void setAvgtime(double avgtime) {
		this.avgtime = avgtime;
	}
	public double getAvgdeep() {
		return avgdeep;
	}
	public void setAvgdeep(double avgdeep) {
		this.avgdeep = avgdeep;
	}
}
