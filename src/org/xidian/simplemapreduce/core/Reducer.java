package org.xidian.simplemapreduce.core;

import java.io.IOException;

import org.xidian.simplemapreduce.io.Writable;
/**
 * Reducer类的接口
 * @param <IK> 输入的key
 * @param <IV> 输入的value
 * @param <OK> 输出的key
 * @param <OV> 输出的value
 * 
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public interface Reducer<IK,IV,OK,OV> {
	/**
	 * reduce操作
	 * @param key 输入的key
	 * @param values 输入的value是个迭代器类型
	 * @param writer 输出类型，比如TextOutputFormat类型
	 * @throws IOException
	 */
	void reduce(IK key,Iterable<IV> values,Writable<OK,OV> writer) throws IOException;
	
}
