package org.xidian.simplemapreduce.test.multidiskfile;

import java.io.IOException;

import org.xidian.simplemapreduce.io.file.MultiDiskFile;
import org.xidian.simplemapreduce.util.TimeUtil;

public class MultiDiskFileFun {

	@SuppressWarnings("rawtypes")
	public static void deleteFile(String path,boolean isFile) throws IOException {

		long start = System.nanoTime();

		MultiDiskFile<?,?> mdlf = new MultiDiskFile(path,isFile);

		if (mdlf.exists()) {
			if (mdlf.isDirectory())
				for (MultiDiskFile<?,?> file : mdlf.listChild()) {
					
					if (file.exists()){
						file.delete();
						System.out.println("delete file:"+file.getPath());
					}else{
						System.out.println("can not find file:"+file.getPath());
					}
				}

			mdlf.delete();
		}

		System.out.print("删除文件所用时间是：" + TimeUtil.getSpanTime(System.nanoTime() - start));
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
//		deleteFile("/a/a",true);
		
//		deleteFile("/a/sort",false);
		deleteFile("/a",false);
	}
}
