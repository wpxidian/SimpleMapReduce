package org.xidian.simplemapreduce.core;


public interface Partition<K> {
	
	int part(K key,int reduceCount);

}
