package org.xidian.simplemapreduce.test.randomfile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.xidian.simplemapreduce.util.TimeUtil;

/**
 * 创建一个文件，文件内容的每行是个随机整数
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class RandomFileCreator {
	
	public static void createFile(String path, int maxLine) throws IOException{
		
		Logger log = LogManager.getLogger(RandomFileCreator.class.getName()) ;
		
		long startTime = System.nanoTime() ;
		
		File file = new File(path) ;
		
		PrintWriter pw = new PrintWriter(new FileOutputStream(file)) ;
		for(int i=0;i<maxLine;i++){
			pw.println(new Random().nextInt(maxLine) + "@" + Math.random()) ;
		}
		
		pw.close();
		long nanoTime = System.nanoTime() - startTime ;
		log.info("创建文件所用的时间是：" + TimeUtil.getSpanTime(nanoTime)) ;
	}
	
	public static void main(String[] args) throws IOException {
		
		createFile("E:/F/hadoopTest/a/a.txt",5000000) ;
		createFile("E:/F/hadoopTest/b/b.txt",1000000) ;
		createFile("E:/F/hadoopTest/c/c.txt",10000) ;
	}

}
