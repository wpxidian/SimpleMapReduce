package org.xidian.simplemapreduce.core.impl;

import org.xidian.simplemapreduce.core.Partition;


public class HashPartition<K> implements Partition<K> {

	@Override
	public int part(K key, int reduceCount) {
		return Math.abs(key.hashCode())%reduceCount;
	}

}
