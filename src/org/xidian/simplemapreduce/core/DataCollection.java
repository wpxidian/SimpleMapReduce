package org.xidian.simplemapreduce.core;


public interface DataCollection<K,V> {
	
	/**
	 * @param key 
	 * @param value
	 */
	void collect(K key,V value);
	
}
