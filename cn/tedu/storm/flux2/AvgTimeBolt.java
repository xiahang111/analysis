package cn.tedu.storm.flux2;

import java.util.ArrayList;
import java.util.HashMap;
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

public class AvgTimeBolt  extends BaseRichBolt {

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
		//3.基于这些数据计算会话的平均时长
		Map<String,List<FluxInfo>> map = new HashMap<>();
		for(FluxInfo fi : list){
			String ssid =fi.getSsid();
			if(map.containsKey(ssid)){
				map.get(ssid).add(fi);
			}else{
				List<FluxInfo> fiList = new ArrayList<>();
				fiList.add(fi);
				map.put(ssid, fiList);
			}
		}
		//4.计算 指标
		int ssCount = map.size();
		int allTime= 0;
		for(Map.Entry<String, List<FluxInfo>> entry : map.entrySet()){
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			List<FluxInfo> fiList = entry.getValue();
			for(FluxInfo fi : fiList){
				long sstime = Long.parseLong(fi.getSstime());
				if(sstime <  minTime) minTime = sstime;
				if(sstime > maxTime) maxTime = sstime;
			}
			allTime += maxTime - minTime;
		}
		double avgTime = 0.0;
		if(ssCount > 0){
			avgTime = Math.round(allTime * 10000.0 / ssCount)/10000.0;
		}
		//5.发送数据
		List<Object> values = input.getValues();
		values.add(avgTime);
		collector.emit(values);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br","avgtime"));
	}

}
