package cn.tedu.storm.flux2;

import java.sql.Timestamp;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tedu.storm.flux.dao.MySqlDao;
import cn.tedu.storm.flux.dao.TongJi3Info;

public class ToMySqlBolt2 extends BaseRichBolt{

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		//1.获取数据封装到bean
		TongJi3Info t3info = new TongJi3Info();
		t3info.setTime(new Timestamp(input.getLongByField("time")));
		t3info.setBr(input.getDoubleByField("br"));
		t3info.setAvgtime(input.getDoubleByField("avgtime"));
		t3info.setAvgdeep(input.getDoubleByField("avgdeep"));
		//2.调用 dao写入数据库
		MySqlDao.getMySqlDao().add3(t3info);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
