package org.xidian.simplemapreduce.core.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.xidian.simplemapreduce.core.BasicDataCollection;
import org.xidian.simplemapreduce.core.Configuration;
import org.xidian.simplemapreduce.core.Partition;

/**
 * 用于存放map阶段从文件块中读取的数据，他包含一个TreeMap的数组，数组大小与reduceCount一致  
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class SortedDataCollection<K,V> extends BasicDataCollection<K,V>{

	public SortedDataCollection(Configuration configuration, Partition<K> partition) {
		super(configuration, partition);
	}

	@Override
	protected void initMaps() {
		for(int i=0;i<datas.length;i++){
			datas[i] = new TreeMap<K,List<V>>();
		}
	}

	@Override
	protected List<V> createList() {
		return new ArrayList<V>(1);
	}

}
