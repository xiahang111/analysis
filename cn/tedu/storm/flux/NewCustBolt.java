package cn.tedu.storm.flux;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tedu.storm.flux.dao.FluxInfo;
import cn.tedu.storm.flux.dao.HBaseDao;

public class NewCustBolt extends BaseRichBolt {
	private OutputCollector collector =  null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			//1.��ȡuvid
			String uvid = input.getStringByField("uvid");
			//2.����uvid��ѯhbase �鿴�����Ƿ����
			List<FluxInfo> list = HBaseDao.getHBaseDao().queryByValue(null,null,"cf1".getBytes(), "uvid".getBytes(), "^"+uvid+"$");
			int newcust = list.size() == 0 ? 1 : 0;
			//3.���ͽ��
			List<Object> values = input.getValues();
			values.add(newcust);
			collector.emit(input,values);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv","vv","newip","newcust"));
	}

}
