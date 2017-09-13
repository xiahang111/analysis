package cn.tedu.storm.flux.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class HBaseDao {
	private static HBaseDao hbaseDao = new HBaseDao();
	private HBaseDao() {
	}
	public static HBaseDao getHBaseDao(){
		return hbaseDao;
	}
	
	public void put(FluxInfo fi){
		HTable tab = null;
		try {
			//1.连接hbase表
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum","Park01:2181,Park02:2181,Park03:2181");
			tab = new HTable(conf, "flux");
			//2.写入数据
			Put put = new Put((fi.getSstime()+"_"+fi.getUvid()+"_"+fi.getSsid()+"_"+(new Random().nextInt(89)+10)).getBytes());
			put.add("cf1".getBytes(), "url".getBytes(), fi.getUrl().getBytes());
			put.add("cf1".getBytes(), "urlname".getBytes(), fi.getUrlname().getBytes());
			put.add("cf1".getBytes(), "uvid".getBytes(), fi.getUvid().getBytes());
			put.add("cf1".getBytes(), "ssid".getBytes(), fi.getSsid().getBytes());
			put.add("cf1".getBytes(), "sscount".getBytes(), fi.getSscount().getBytes());
			put.add("cf1".getBytes(), "sstime".getBytes(), fi.getSstime().getBytes());
			put.add("cf1".getBytes(), "cip".getBytes(), fi.getCip().getBytes());
			tab.put(put);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(tab != null){
				try {
					tab.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					tab = null;
				}
			}
		}
	}
	
	public List<FluxInfo> queryByValue(byte [] start,byte [] stop,byte [] cf,byte [] c,String valueRegex){
		HTable tab = null;
		try {
			//1.连接hbase表
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum","Park01:2181,Park02:2181,Park03:2181");
			tab = new HTable(conf, "flux");
			//2.根据正则查询表
			Scan scan = new Scan();
			if(start != null) scan.setStartRow(start);
			if(stop != null) scan.setStopRow(stop);
			if(valueRegex != null) scan.setFilter(new SingleColumnValueFilter(cf, c, CompareOp.EQUAL, new RegexStringComparator(valueRegex)));
			ResultScanner rs = tab.getScanner(scan);
			//3.遍历结果 封装到对象集合中
			List<FluxInfo> resultList = new ArrayList<>();
			Iterator<Result> it = rs.iterator();
			while(it.hasNext()){
				Result r = it.next();
				FluxInfo fi = new FluxInfo();
				fi.setUrl(new String(r.getValue("cf1".getBytes(), "url".getBytes())));
				fi.setUrlname(new String(r.getValue("cf1".getBytes(), "urlname".getBytes())));
				fi.setUvid(new String(r.getValue("cf1".getBytes(), "uvid".getBytes())));
				fi.setSsid(new String(r.getValue("cf1".getBytes(), "ssid".getBytes())));
				fi.setSscount(new String(r.getValue("cf1".getBytes(), "sscount".getBytes())));
				fi.setSstime(new String(r.getValue("cf1".getBytes(), "sstime".getBytes())));
				fi.setCip(new String(r.getValue("cf1".getBytes(), "cip".getBytes())));
				resultList.add(fi);
			}
			
			return resultList;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(tab != null){
				try {
					tab.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					tab = null;
				}
			}
		}
	}
	
	public List<FluxInfo> query(byte [] start,byte [] stop,String rkRegex){
		HTable tab = null;
		try {
			//1.连接hbase表
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum","Park01:2181,Park02:2181,Park03:2181");
			tab = new HTable(conf, "flux");
			//2.根据正则查询表
			Scan scan = new Scan();
			if(start != null) scan.setStartRow(start);
			if(stop != null) scan.setStopRow(stop);
			if(rkRegex != null) scan.setFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rkRegex)));
			ResultScanner rs = tab.getScanner(scan);
			//3.遍历结果 封装到对象集合中
			List<FluxInfo> resultList = new ArrayList<>();
			Iterator<Result> it = rs.iterator();
			while(it.hasNext()){
				Result r = it.next();
				FluxInfo fi = new FluxInfo();
				fi.setUrl(new String(r.getValue("cf1".getBytes(), "url".getBytes())));
				fi.setUrlname(new String(r.getValue("cf1".getBytes(), "urlname".getBytes())));
				fi.setUvid(new String(r.getValue("cf1".getBytes(), "uvid".getBytes())));
				fi.setSsid(new String(r.getValue("cf1".getBytes(), "ssid".getBytes())));
				fi.setSscount(new String(r.getValue("cf1".getBytes(), "sscount".getBytes())));
				fi.setSstime(new String(r.getValue("cf1".getBytes(), "sstime".getBytes())));
				fi.setCip(new String(r.getValue("cf1".getBytes(), "cip".getBytes())));
				resultList.add(fi);
			}
			
			return resultList;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(tab != null){
				try {
					tab.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					tab = null;
				}
			}
		}
	}
	
}
