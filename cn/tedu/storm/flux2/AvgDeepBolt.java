package cn.tedu.storm.flux2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tedu.storm.flux.dao.FluxInfo;
import cn.tedu.storm.flux.dao.HBaseDao;

public class AvgDeepBolt extends BaseRichBolt {

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
		//3.基于这些数据计算会话的平均深度 
		Map<String,Set<String>> map = new HashMap<>();
		for(FluxInfo fi : list){
			if(map.containsKey(fi.getSsid())){
				map.get(fi.getSsid()).add(fi.getUrlname());
			}else{
				Set<String> set = new HashSet<>();
				set.add(fi.getUrlname());
				map.put(fi.getSsid(), set);
			}
		}
		//4.计算 指标
		int ssCount = map.size();
		int allDeep = 0;
		for(Map.Entry<String, Set<String>> entry : map.entrySet()){
			allDeep += entry.getValue().size();
		}
		double avgDeep = 0.0;
		if(ssCount > 0){
			avgDeep = Math.round(allDeep * 10000.0 / ssCount)/10000.0;
		}
		//5.发送数据
		List<Object> values = input.getValues();
		values.add(avgDeep);
		collector.emit(values);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br","avgtime","avgdeep"));
	}

}
