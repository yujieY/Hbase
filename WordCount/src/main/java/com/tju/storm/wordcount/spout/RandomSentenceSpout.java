package com.tju.storm.wordcount.spout;

import java.util.Map;
import java.util.Random;

import javax.rmi.CORBA.UtilDelegate;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

//随机发送一条内置消息
public class RandomSentenceSpout extends BaseRichSpout{
	
	SpoutOutputCollector spoutOutputCollector;
	Random random;

	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(2000);
		String[] sentences = new String[]{};
		String sentence = sentences[random.nextInt(sentences.length)];
		//使用emit方法进行tuple发布
		spoutOutputCollector.emit(new Values(sentence.trim().toLowerCase()));
	}
	
	//消息保证机制中的ack确认方法
	public void ack(Object id){
	}
	
	//消息保证机制中的fail确认方法
	public void fail(Object id){
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		spoutOutputCollector = collector;
		random = new Random();
	}

	//字段声明
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		// TODO Auto-generated method stub
		declare.declare(new Fields("word"));
	}
	

}
