package cn.tedu.storm.flux.dao;

public class FluxInfo {
	private String url;
	private String urlname;
	private String uvid;
	private String ssid;
	private String sscount;
	private String sstime;
	private String cip;
	
	public FluxInfo() {
	}
	
	public FluxInfo(String url, String urlname, String uvid, String ssid, String sscount, String sstime, String cip) {
		this.url = url;
		this.urlname = urlname;
		this.uvid = uvid;
		this.ssid = ssid;
		this.sscount = sscount;
		this.sstime = sstime;
		this.cip = cip;
	}


	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUrlname() {
		return urlname;
	}
	public void setUrlname(String urlname) {
		this.urlname = urlname;
	}
	public String getUvid() {
		return uvid;
	}
	public void setUvid(String uvid) {
		this.uvid = uvid;
	}
	public String getSsid() {
		return ssid;
	}
	public void setSsid(String ssid) {
		this.ssid = ssid;
	}
	public String getSscount() {
		return sscount;
	}
	public void setSscount(String sscount) {
		this.sscount = sscount;
	}
	public String getSstime() {
		return sstime;
	}
	public void setSstime(String sstime) {
		this.sstime = sstime;
	}
	public String getCip() {
		return cip;
	}
	public void setCip(String cip) {
		this.cip = cip;
	}
	
}
