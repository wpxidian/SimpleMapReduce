package org.xidian.simplemapreduce.test.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Set;
import java.util.TreeSet;

import org.xidian.simplemapreduce.util.TimeUtil;
/**
 * 直接排序与MapReduce对比
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class SortJobAdversary {
	
	public static void sortJobAdversary(String inputPath ,String inputFile ,String outputPath,int bufferSize) throws NumberFormatException, IOException{
		
		long soloStart = System.nanoTime();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputPath+inputFile))),bufferSize);
		
		Set<Data> set = new TreeSet<Data>();
		
		String line;
		while((line = br.readLine())!=null){
			String[] strs = line.split("@");
			set.add(new Data(Integer.parseInt(strs[0]), Double.parseDouble(strs[1])));
		}
		
		br.close();
		
		System.out.print("从文件中将数据读入Set所用时间:" + TimeUtil.getSpanTime(System.nanoTime()-soloStart));
		
		long writeTime = System.nanoTime();
				
		File outputDir = new File(inputPath+outputPath);
		outputDir.mkdirs();
		File outputFile = new File(inputPath+outputPath+"/output.txt");
		outputFile.createNewFile();
		
		PrintWriter pw = new PrintWriter(outputFile);
		
		for(Data data : set){
			pw.println(data);
		}
		
		pw.close();
		
		System.out.print("将Set中的数据写入文件所用的时间：" + TimeUtil.getSpanTime(System.nanoTime()-writeTime));
		System.out.print("总共花费的时间：" + TimeUtil.getSpanTime(System.nanoTime()-soloStart));
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		sortJobAdversary("E:/F/hadoopTest/a/","a.txt" ,"sort-output-adersary", 40960);
		
	}
	
}
