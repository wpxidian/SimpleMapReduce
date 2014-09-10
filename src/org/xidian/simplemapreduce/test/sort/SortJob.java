package org.xidian.simplemapreduce.test.sort;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.xidian.simplemapreduce.core.BasicDataReader;
import org.xidian.simplemapreduce.core.Configuration;
import org.xidian.simplemapreduce.core.DataCollection;
import org.xidian.simplemapreduce.core.FrameworkDataCollection;
import org.xidian.simplemapreduce.core.Mapper;
import org.xidian.simplemapreduce.core.Partition;
import org.xidian.simplemapreduce.core.impl.HashDataCollection;
import org.xidian.simplemapreduce.core.impl.HashDataReader;
import org.xidian.simplemapreduce.core.impl.HashPartition;
import org.xidian.simplemapreduce.core.impl.JobBuilder;
import org.xidian.simplemapreduce.core.impl.SortedDataCollection;
import org.xidian.simplemapreduce.core.impl.SortedDataReader;
import org.xidian.simplemapreduce.io.data.Nothing;
import org.xidian.simplemapreduce.io.impl.TextInputFormat;
import org.xidian.simplemapreduce.io.impl.TextOutputFormat;
/**
 * 使用MapReduce模拟数字排序工作
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class SortJob {
	
	public static class SortMapper implements Mapper<Long,String,Data,Nothing>{

		@Override
		public void map(Long key, String value,DataCollection<Data, Nothing> collection) {
			
			String[] strs = value.split("@");
			collection.collect(new Data(Integer.parseInt(strs[0]),Double.parseDouble(strs[1])), Nothing.INSTANCE);
			//collection.collect(new Data(Integer.parseInt(value), 0.0), Nothing.INSTANCE);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws 
			IOException, 
			NoSuchMethodException,
			SecurityException, 
			InstantiationException, 
			IllegalAccessException, 
			IllegalArgumentException, 
			InvocationTargetException, 
			InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Data,Nothing,Data,Nothing> jobBuilder = new JobBuilder<Long,String,Data,Nothing,Data,Nothing>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Data, Nothing>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(SortMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Data, Nothing>>) SortedDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Data>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Data, Nothing>>) SortedDataReader.class);
		
		jobBuilder.createJob().excute();
	}
	
	
	@SuppressWarnings("unchecked")
	public static void startJobHash(String inputPath,String fileName,String outputPath) 
			throws IOException, 
			NoSuchMethodException, 
			SecurityException, 
			InstantiationException, 
			IllegalAccessException,
			IllegalArgumentException, 
			InvocationTargetException, 
			InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Data,Nothing,Data,Nothing> jobBuilder = new JobBuilder<Long,String,Data,Nothing,Data,Nothing>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Data, Nothing>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(SortMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Data, Nothing>>) HashDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Data>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Data, Nothing>>) HashDataReader.class);
		
		jobBuilder.createJob().excute();
	}
	
	public static void main(String[] args) throws 
			IOException, 
			InterruptedException, 
			NoSuchMethodException, 
			SecurityException, 
			InstantiationException,
			IllegalAccessException, 
			IllegalArgumentException, 
			InvocationTargetException {
		
//		PrintableSleep.sleep(15);
		startJob("E:/F/hadoopTest/a/","a.txt","sort-output-0.3");
		
//		startJobHash("E:/hadoop_test/a/","a.txt","sort-output-0.3");
	}

}
