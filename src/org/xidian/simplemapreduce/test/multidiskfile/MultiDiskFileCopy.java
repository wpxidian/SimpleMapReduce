package org.xidian.simplemapreduce.test.multidiskfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.xidian.simplemapreduce.io.file.MultiDiskLineFile;
import org.xidian.simplemapreduce.util.TimeUtil;


public class MultiDiskFileCopy {

	public static void copyFile(String fromPath,String toPath,int bufferSize) throws IOException {

		long start = System.nanoTime();

		final File file = new File(fromPath);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(file)),bufferSize);

		MultiDiskLineFile mdlf = new MultiDiskLineFile(toPath);
		
		mdlf.createFileORDir();
		
		try {
			String str = null;
			int i = 0;
			while ((str = br.readLine()) != null) {
				i++;
				String[] strs = str.split("@");
				mdlf.write(strs[0],strs[1]);
			}
			System.out.println(i);
		} finally {
			mdlf.close();
			br.close();
		}

		System.out.print("复制文件所用的时间:" + TimeUtil.getSpanTime(System.nanoTime() - start));
	}
	
	public static void deleteFile(String path) throws IOException{

		long start = System.nanoTime();
		
		MultiDiskLineFile mdlf = new MultiDiskLineFile(path);
		if(mdlf.exists())
			mdlf.delete();
		
		System.out.print("删除文件所用的时间:" + TimeUtil.getSpanTime(System.nanoTime() - start));
	}
	

	public static void main(String[] args) throws IOException, InterruptedException {
		//Thread.sleep(10000);
		copyFile("E:/F/hadoopTest/a/a.txt","/a/a",40960);
		//deleteFile("/a/a");
	}
}
