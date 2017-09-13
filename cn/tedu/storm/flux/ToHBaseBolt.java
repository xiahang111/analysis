package cn.tedu.storm.flux;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tedu.storm.flux.dao.FluxInfo;
import cn.tedu.storm.flux.dao.HBaseDao;

public class ToHBaseBolt extends BaseRichBolt {

	private  OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector  = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			//1.获取tuple中的信息封装到bean中
			String url = input.getStringByField("url");
			String urlname = input.getStringByField("urlname");
			String uvid = input.getStringByField("uvid");
			String ssid = input.getStringByField("ssid");
			String sscount = input.getStringByField("sscount");
			String sstime = input.getStringByField("sstime");
			String cip = input.getStringByField("cip");
			FluxInfo fi = new FluxInfo(url, urlname, uvid, ssid, sscount, sstime, cip);
			//2.调用dao将bean中的数据存入hbase
			HBaseDao.getHBaseDao().put(fi);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
