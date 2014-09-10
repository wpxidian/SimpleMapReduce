package org.xidian.simplemapreduce.io;
/**
 * MapReduce输入格式的接口
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public abstract class InputFormat<K,V> implements Splittable<K,V>{
	
	protected final String path;
	
	public InputFormat(String path){
		this.path = path;
	}

}
