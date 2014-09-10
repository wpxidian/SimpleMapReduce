package org.xidian.simplemapreduce.core.impl;

import org.xidian.simplemapreduce.core.DataCollection;
import org.xidian.simplemapreduce.core.Mapper;
/**
 * 简单的Map实现类，执行map操作
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
@SuppressWarnings("rawtypes")
public class SimpleMapper implements Mapper{
	@SuppressWarnings("unchecked")
	@Override
	public void map(Object key, Object value, DataCollection colletion) {
		colletion.collect(key, value);
	}
}
