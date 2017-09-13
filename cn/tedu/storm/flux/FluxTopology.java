package cn.tedu.storm.flux;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cn.tedu.storm.flux2.AvgDeepBolt;
import cn.tedu.storm.flux2.AvgTimeBolt;
import cn.tedu.storm.flux2.BrBolt;
import cn.tedu.storm.flux2.TimeBolt;
import cn.tedu.storm.flux2.ToMySqlBolt2;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FluxTopology {
	public static void main(String[] args) throws Exception {
		/**
		 * 实时 拓扑
		 */
		//1.创建组件
		BrokerHosts hosts = new ZkHosts("Park01:2181,Park02:2181,Park03:2181");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "flux", "/flux" , UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		ClearBolt clearBolt = new ClearBolt();
		PrintBolt printBolt = new PrintBolt();
		PvBolt pvBolt =  new PvBolt();
		UvBolt uvBolt = new  UvBolt();
		VvBolt vvBolt = new VvBolt();
		NewIpBolt newIpBolt = new NewIpBolt();
		NewCustBolt newCustBolt = new NewCustBolt();
		ToMySqlBolt toMySqlBolt = new ToMySqlBolt();
		ToHBaseBolt toHbaseBolt = new ToHBaseBolt();
		
		//2.创建构建者
		TopologyBuilder builder = new TopologyBuilder();
		
		//3.组织 拓扑
		builder.setSpout("Flux_Spout", kafkaSpout);
		builder.setBolt("Clear_Bolt", clearBolt).shuffleGrouping("Flux_Spout");
		builder.setBolt("Pv_Bolt", pvBolt).shuffleGrouping("Clear_Bolt");
		builder.setBolt("Uv_Bolt", uvBolt).shuffleGrouping("Pv_Bolt");
		builder.setBolt("Vv_Bolt", vvBolt).shuffleGrouping("Uv_Bolt");
		builder.setBolt("New_Ip_Bolt", newIpBolt).shuffleGrouping("Vv_Bolt");
		builder.setBolt("New_Cust_Bolt", newCustBolt).shuffleGrouping("New_Ip_Bolt");
		builder.setBolt("To_MySql_Bolt", toMySqlBolt).shuffleGrouping("New_Cust_Bolt");
		builder.setBolt("To_HBase_Bolt", toHbaseBolt).shuffleGrouping("New_Cust_Bolt");
		builder.setBolt("Print_Bolt", printBolt).shuffleGrouping("New_Cust_Bolt");
		
		//4.创建拓扑
		StormTopology topology = builder.createTopology();
		
		//5.提交拓扑到 集群中运行
		Config conf = new  Config();
		StormSubmitter.submitTopology("FluxTopology", conf , topology);
	
		
		/**
		 * 伪实时  拓扑
		 */
		TimeBolt timeBolt = new TimeBolt();
		BrBolt brBolt = new BrBolt();
		AvgTimeBolt avgTimeBolt = new AvgTimeBolt();
		AvgDeepBolt avgDeepBolt = new AvgDeepBolt();
		ToMySqlBolt2 toMySqlBolt2 = new ToMySqlBolt2();
		PrintBolt printBolt2  = new PrintBolt();
		
		TopologyBuilder builder2 = new TopologyBuilder();
		builder2.setBolt("Time_Bolt", timeBolt);
		builder2.setBolt("Br_Bolt", brBolt).shuffleGrouping("Time_Bolt");
		builder2.setBolt("Avg_Time_Bolt", avgTimeBolt).shuffleGrouping("Br_Bolt");
		builder2.setBolt("Avg_Deep_Bolt", avgDeepBolt).shuffleGrouping("Avg_Time_Bolt");
		builder2.setBolt("To_MySql_Bolt2", toMySqlBolt2).shuffleGrouping("Avg_Deep_Bolt");
		builder2.setBolt("Print_Bolt2", printBolt2).shuffleGrouping("Avg_Deep_Bolt");
		
		StormTopology topology2 = builder2.createTopology();
		
		Config conf2 = new  Config();
		StormSubmitter.submitTopology("FluxTopology2", conf2, topology2);
	
	}
}
