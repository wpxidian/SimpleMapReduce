package org.xidian.simplemapreduce.io;

import java.io.IOException;

/**
 * 文件块的读取类
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public interface Readable<K, V> {

	/**
	 * 判断文件块中是否还有下一个数据
	 * @return 判断文件块中是否还有下一个数据
	 * @throws IOException 
	 */
	boolean next() throws IOException;

	/**
	 * @return 返回从文件块中读取的数据的key
	 */
	K getKey();

	/**
	 * @return 返回从文件块中读取的数据
	 */
	V getValue();
}
