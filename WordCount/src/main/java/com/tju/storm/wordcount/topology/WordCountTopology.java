package com.tju.storm.wordcount.topology;

import com.tju.storm.wordcount.bolt.PrintBolt;
import com.tju.storm.wordcount.bolt.WordCountBolt;
import com.tju.storm.wordcount.bolt.WordNormalizerBolt;
import com.tju.storm.wordcount.spout.RandomSentenceSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopology {

	private static TopologyBuilder builder = new TopologyBuilder();

	public static void main(String[] args) {
		Config config = new Config();

		builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
		builder.setBolt("WordNormalizer", new WordNormalizerBolt(), 2)
				.shuffleGrouping("RandomSentence");
		builder.setBolt("WordCount", new WordCountBolt(), 2).shuffleGrouping(
				"WordNormalizer");
		builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping(
				"WordCount");
		
		config.setDebug(false);
		
		if(args != null && args.length > 0){
			config.setNumWorkers(1);
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
			config.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount", config, builder.createTopology());
		}
	}
}
