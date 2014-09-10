package org.xidian.simplemapreduce.core;

/**
 * Mapper类的接口
 * @param <IK> 输入的key
 * @param <IV> 输入的value
 * @param <OK> map输出的key
 * @param <OV> map输出的value
 * 
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public interface Mapper<IK,IV,OK,OV> {
	
	/**
	 * map操作
	 * @param key 输入的key
	 * @param value 输入的value
	 * @param collection 输出是DataCollection类型
	 */
	void map(IK key,IV value,DataCollection<OK,OV> collection);
}
