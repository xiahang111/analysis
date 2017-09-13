package cn.tedu.storm.flux2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tedu.storm.flux.dao.FluxInfo;
import cn.tedu.storm.flux.dao.HBaseDao;

public class BrBolt extends BaseRichBolt{

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector  = collector;
	}

	@Override
	public void execute(Tuple input) {
		//1.获取触发的时间　计算得到　时间区间
		long endtime = input.getLongByField("time");
		long starttime =endtime - 1000 * 3600;
		//2.查询hbase获取这个时间区间内的所有数据
		List<FluxInfo> list = HBaseDao.getHBaseDao().query((starttime+"").getBytes(), (endtime+"").getBytes(), null);
		//3.基于这些数据计算跳出率
		Map<String,Integer> map = new HashMap<>();
		for(FluxInfo fi : list){
			String ssid = fi.getSsid();
			map.put(ssid, map.containsKey(ssid) ?  map.get(ssid) + 1 :  1);
		}
		
		int ssCount = map.size();
		int brCount = 0;
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			if(entry.getValue() == 1) brCount++;
		}
		
		double br = 0.0;
		if(ssCount > 0){
			br = Math.round(brCount * 10000.0 / ssCount) / 10000.0;
		}
		
		//4.发送 结果
		List<Object> values = input.getValues();
		values.add(br);
		collector.emit(values);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br"));
	}

}
