package org.xidian.simplemapreduce.core.impl;

import java.io.IOException;

import org.xidian.simplemapreduce.core.Reducer;
import org.xidian.simplemapreduce.io.Writable;

public class StringReducer<K,V> implements Reducer<K,V,String,String>{


	@Override
	public void reduce(K key, Iterable<V> values,
			Writable<String, String> writer) throws IOException {
		
		for(V value : values)
			writer.write(key.toString(), value.toString());
		
	}
	
}
