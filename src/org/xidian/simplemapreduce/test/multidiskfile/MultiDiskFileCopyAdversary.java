package org.xidian.simplemapreduce.test.multidiskfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.xidian.simplemapreduce.util.TimeUtil;

public class MultiDiskFileCopyAdversary {

	public static void copyFile(String fromPath,String toPath,int bufferSize) throws IOException {

		long start = System.nanoTime();

		final File fromFile = new File(fromPath);
		
		final File toFile = new File(toPath);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(fromFile)),bufferSize);

		PrintWriter pw = new PrintWriter(toFile);
		
		toFile.createNewFile();
		
		try {
			String str = null;
			int i = 0;
			while ((str = br.readLine()) != null) {
				i++;
				pw.println(i+"@"+str);
			}
			
			System.out.println(i);
		} finally {
			pw.close();
			br.close();
		}

		System.out.print("复制文件所用的时间:" + TimeUtil.getSpanTime(System.nanoTime() - start));
	}
	

	public static void main(String[] args) throws IOException, InterruptedException {
		copyFile("E:/F/hadoopTest/a/a.txt","E:/F/hadoop_test/a/a(2).txt",4096);
	}
}
