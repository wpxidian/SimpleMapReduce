package org.xidian.simplemapreduce.util;

import java.io.IOException;
import java.util.Properties;
/**
 * 读取属性文件
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class PropertiesConfigurationReader {
	
	private final Properties properties;
	
	public PropertiesConfigurationReader(Class<?> useClass,String propertiesName) throws IOException
	{
		properties = new Properties();
		properties.load(useClass.getClassLoader().getResourceAsStream(propertiesName));
	}
	
	public String getValue(String key)
	{
		return properties.getProperty(key, null);
	}
	
	public int getInt(String key){
		return Integer.parseInt(properties.getProperty(key, null));
	}
	
	public long getLong(String key){
		return Long.parseLong(properties.getProperty(key, null));
	}
	
	public char getChar(String key){
		return properties.getProperty(key, null).charAt(0);
	}
	
}
