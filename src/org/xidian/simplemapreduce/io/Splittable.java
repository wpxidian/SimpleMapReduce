package org.xidian.simplemapreduce.io;

import java.util.Collection;
/**
 * 实现该接口的类，表明该类所封装的文件是可以拆分成块的文件
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public interface Splittable<K,V> {
	
	/**
	 * 文件划分成块的个数
	 * @return 返回文件划分的块数
	 */
	int getFragmentCount();
	
	/**
	 * 获取所有分块文件对应的文件阅读器的集合
	 * @return 获取所有分块文件对应的文件阅读器的集合
	 */
	Collection<? extends Readable<K,V>> getReaders();
	
	
}
