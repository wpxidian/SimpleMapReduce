package org.xidian.simplemapreduce.core.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.xidian.simplemapreduce.core.BasicDataCollection;
import org.xidian.simplemapreduce.core.Configuration;
import org.xidian.simplemapreduce.core.Partition;


public class HashDataCollection<K,V> extends BasicDataCollection<K, V>{
	
	public HashDataCollection(Configuration configuration, Partition<K> partition) {
		super(configuration, partition);
	}

	@Override
	protected void initMaps() {
		for(int i=0;i<datas.length;i++){
			datas[i] = new HashMap<K,List<V>>();
		}
	}

	@Override
	protected List<V> createList() {
		return new ArrayList<V>();
	}

}
