package org.xidian.simplemapreduce.core.impl;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.xidian.simplemapreduce.core.Reducer;
import org.xidian.simplemapreduce.io.Writable;
/**
 * 简单的Reduce实现类，执行reduce操作
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
@SuppressWarnings("rawtypes")
public class SimpleReducer implements Reducer{

	@SuppressWarnings("unchecked")
	@Override
	public void reduce(Object key, Iterable values,
			Writable writer) throws FileNotFoundException, IOException {
		try{
		writer.write(key, values.iterator().next());
		}catch(NullPointerException r){
			System.out.println(key +"  "+values+"  ");
		}
		
	}
	
}
