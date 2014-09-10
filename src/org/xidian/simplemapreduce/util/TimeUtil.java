package org.xidian.simplemapreduce.util;

import org.apache.log4j.Logger;
/**
 * 工具类，用于打印时间信息
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class TimeUtil {
	
	public static String getSpanTime(long nanoTime){
		
		int nano = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int micro = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int ms = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int s = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int min = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int h = (int) (nanoTime%24);
		nanoTime = nanoTime/24;
		int d = (int) nanoTime;
		
		return d + "天" + h + "小时" + min + "分钟" + s + "秒" + ms + "毫秒" + micro + "微秒" + nano + "纳秒";
	}
	
	public static void printNanoTimeByLoggerInfo(Logger logger,String message,long nanoTime)
	{
		int nano = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int micro = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int ms = (int) (nanoTime%1000);
		nanoTime = nanoTime/1000;
		int s = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int min = (int) (nanoTime%60);
		nanoTime = nanoTime/60;
		int h = (int) (nanoTime%24);
		nanoTime = nanoTime/24;
		int d = (int) nanoTime;
		
		logger.info(message + ":" + d + "天" + h + "小时" + min + "分钟" + s + "秒" + ms + "毫秒" + micro + "微秒" + nano + "纳秒");
	}
}
