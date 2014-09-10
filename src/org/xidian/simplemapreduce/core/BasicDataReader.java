package org.xidian.simplemapreduce.core;


import java.util.List;
import java.util.Map;

import org.xidian.simplemapreduce.io.Readable; 

public abstract class BasicDataReader<K,V> implements Readable<K, List<V>> {

	protected final Map<K, List<V>>[] maps;
	
	public BasicDataReader(Map<K, List<V>>[] maps){
		this. maps = maps;
	}
	

}
