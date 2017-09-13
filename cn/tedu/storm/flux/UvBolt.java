package cn.tedu.storm.flux;

import java.util.Calendar;
import java.util.Date;
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

public class UvBolt extends BaseRichBolt {
	private OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			//1.��ȡ��ǰtuple��uvid
			String uvid = input.getStringByField("uvid");
			//2.��ѯhbase ��ѯ����֮ǰ���������Ƿ������� uvid������
			Calendar c = Calendar.getInstance();
			c.setTime(new Date());
			c.set(Calendar.HOUR, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			long startTime = c.getTime().getTime();
			long endTime = startTime + 1000 * 60 * 60 * 24;
			List<FluxInfo> list = HBaseDao.getHBaseDao().query((startTime+"").getBytes(),(endTime+"").getBytes(),"^\\d+_"+uvid+"_\\d+_\\d+$");
			//3.��������� ��uvΪ1 ����Ϊ0
			int uv = list.size() == 0 ? 1 : 0;
			//4.д�����
			List<Object> values = input.getValues();
			values.add(uv);
			collector.emit(input,values);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv"));
	}

}
