package com.tju.storm.wordcount.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 消息标准化处理
 * */

public class WordNormalizerBolt implements IRichBolt{

	private OutputCollector outputCollector;
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	//执行订阅的Tuple逻辑处理的方法
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		
		for(String word:words){
			outputCollector.emit(new Values(words));
		}
	}

	//bolt初始化方法
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
	}

	//字段声明
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		// TODO Auto-generated method stub
		declare.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
