package com.tju.storm.wordcount.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Map≈≈–Ú
 * */
public class MapSort {
	
	public static Map<String, Integer> sortByValue(Map<String, Integer>map){
		
		if(map == null){
			return null;
		}
		
		List list = new LinkedList(map.entrySet());
		
		Collections.sort(list,new Comparator() {

			public int compare(Object o1, Object o2) {
				// TODO Auto-generated method stub
				Comparable sort1 = (Comparable) ((Map.Entry) o1).getValue();
				Comparable sort2 = (Comparable) ((Map.Entry) o2).getValue();
				return sort2.compareTo(sort1);
			}
		});
		
		Map result = new LinkedHashMap();
		
		for(Iterator it = list.iterator();it.hasNext();){
			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(),entry.getValue());
		}
		return result;
	}
}
