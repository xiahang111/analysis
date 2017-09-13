package cn.tedu.storm.flux;

import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tedu.storm.flux.dao.MySqlDao;
import cn.tedu.storm.flux.dao.TongJi2Info;

public class ToMySqlBolt extends BaseRichBolt{

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			//1.获取统计指标
			Date time = new Date(Long.parseLong(input.getStringByField("sstime")));
			int pv  = input.getIntegerByField("pv");
			int uv  = input.getIntegerByField("uv");
			int vv  = input.getIntegerByField("vv");
			int newip  = input.getIntegerByField("newip");
			int newcust  = input.getIntegerByField("newcust");
			TongJi2Info t2info  = new TongJi2Info(time, pv, uv, vv, newip, newcust);
			
			//2.将数据写入数据库
			MySqlDao.getMySqlDao().add2(t2info);
			collector.ack(input);
		} catch (NumberFormatException e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
