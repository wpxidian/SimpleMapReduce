package org.xidian.simplemapreduce.test.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.xidian.simplemapreduce.util.TimeUtil;

public class FileCopy {
	
	public static void copyFile(String fromPath,String toPath,int bufferSize) throws IOException {

		long start = System.nanoTime();

		final File file = new File(fromPath);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(file)),bufferSize);

		MultiLineFile mdlf = new MultiLineFile(toPath);
		
		mdlf.createFileORDir();
		
		try {
			String str = null;
			int i = 0;
			while ((str = br.readLine()) != null) {
				i++;
				mdlf.write(str,null);
			}
			System.out.println(i);
		} finally {
			mdlf.close();
			br.close();
		}

		System.out.print("复制文件所用的时间:" + TimeUtil.getSpanTime(System.nanoTime() - start));
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		//Thread.sleep(10000);
		copyFile("E:/F/hadoopTest/word/muguang.txt","/b/b",4096);
		//deleteFile("/a/a");
	}
}
