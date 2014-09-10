package org.xidian.simplemapreduce.io;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
/**
 * 实现该接口的类是可以输出文件的类
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public interface Writable<K,V> extends Closeable{
	
	/**
	 * 用输出流写数据
	 * @throws FileNotFoundException 
	 * @throws IOException 
	 */
	void write(K key,V value) throws IOException;
	
	/**
	 * 关闭输出流
	 */
	@Override
	void close();	
}
