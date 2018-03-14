package com.tju.storm.wordcount.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 打印接受的数据
 * */
public class PrintBolt extends BaseBasicBolt{

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String mesg = input.getString(0);
		System.out.println(mesg);
	}

	public void declareOutputFields(OutputFieldsDeclarer declare) {
		// TODO Auto-generated method stub
		declare.declare(new Fields("word"));
	}

}
