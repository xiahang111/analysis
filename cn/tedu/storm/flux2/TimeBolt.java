package cn.tedu.storm.flux2;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TimeBolt extends BaseRichBolt{

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new  Config();
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3600);
		return conf;
	}
	
	private OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long time = System.currentTimeMillis();
		collector.emit(new Values(time));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time"));
	}

}
