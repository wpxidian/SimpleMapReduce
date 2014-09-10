package org.xidian.simplemapreduce.test.sort;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.xidian.simplemapreduce.core.BasicDataReader;
import org.xidian.simplemapreduce.core.Configuration;
import org.xidian.simplemapreduce.core.DataCollection;
import org.xidian.simplemapreduce.core.FrameworkDataCollection;
import org.xidian.simplemapreduce.core.Mapper;
import org.xidian.simplemapreduce.core.Partition;
import org.xidian.simplemapreduce.core.Reducer;
import org.xidian.simplemapreduce.core.impl.HashPartition;
import org.xidian.simplemapreduce.core.impl.JobBuilder;
import org.xidian.simplemapreduce.core.impl.SortedDataCollection;
import org.xidian.simplemapreduce.core.impl.SortedDataReader;
import org.xidian.simplemapreduce.core.impl.StringReducer;
import org.xidian.simplemapreduce.io.data.Nothing;
import org.xidian.simplemapreduce.io.file.MultiDiskLineFile;
/**
 * 模拟分布式文件系统进行排序
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class SortJobWithMultidisk {
	
	public static class SortMapper implements Mapper<String,String,Data,Nothing>{

		@Override
		public void map(String key, String value,DataCollection<Data, Nothing> collection) {
			collection.collect(new Data(Integer.parseInt(key),Double.parseDouble(value)), Nothing.INSTANCE);
		}
		
	}
	
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<String,String,Data,Nothing,String,String> jobBuilder = new JobBuilder<String,String,Data,Nothing,String,String>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(MultiDiskLineFile.class);
		
		jobBuilder.setOutputFormatClass(MultiDiskLineFile.class);
		
		jobBuilder.setMapperClass(SortMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Data, Nothing>>) SortedDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Data>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Data, Nothing>>) SortedDataReader.class);
		
		jobBuilder.setReducerClass((Class<? extends Reducer<Data, Nothing, String, String>>) StringReducer.class);
		
		jobBuilder.createJob().excute();
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

//		PrintableSleep.sleep(15);
		
		startJob("/a/","a","sort");
		
	}

}
