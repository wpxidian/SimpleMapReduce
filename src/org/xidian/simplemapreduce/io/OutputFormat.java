package org.xidian.simplemapreduce.io;

/**
 * MapReduce输出格式接口
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public abstract class OutputFormat<K,V> implements Writable<K,V>{
	
	protected final String path;
	
	public OutputFormat(String path){
		this.path = path;
	}

}
