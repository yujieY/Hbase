package com.tju.storm.wordcount.bolt;

import java.util.HashMap;
import java.util.Map;

import org.omg.CORBA.ValueBaseHolder;

import com.tju.storm.wordcount.util.MapSort;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * 单词统计，并实时获取词频前N的发射出去
 * */
public class WordCountBolt implements IRichBolt{
	
	Map<String, Integer> counters;
	OutputCollector outputCollector;

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String str = input.getString(0);
		
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer i = counters.get(str) + 1;
			counters.put(str, i);
		}
		
		//取词频最大的前10个
		int num = 10;
		int length = 0;
		
		//使用MapSort对map进行排序
		counters = MapSort.sortByValue(counters);
		
		if(num < counters.keySet().size()){
			length = num;
		}else{
			length = counters.keySet().size();
		}
		
		String word = null;
		
		//增量统计
		int count = 0;
		for(String key:counters.keySet()){
			if(count >= length){
				break;
			}
			if(count == 0){
				word = "["+key+":"+counters.get(key)+"]";
			}else{
				word = word +",["+key+":"+counters.get(key)+"]";
			}
			count ++;
		}
		
		word = "The first"+num+":"+word;
		outputCollector.emit(new Values(word));
	}

	public void prepare(Map stormConf, TopologyContext text, OutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		counters = new HashMap<String, Integer>();
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
