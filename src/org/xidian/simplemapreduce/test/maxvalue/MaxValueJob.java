package org.xidian.simplemapreduce.test.maxvalue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.xidian.simplemapreduce.core.BasicDataReader;
import org.xidian.simplemapreduce.core.Configuration;
import org.xidian.simplemapreduce.core.DataCollection;
import org.xidian.simplemapreduce.core.FrameworkDataCollection;
import org.xidian.simplemapreduce.core.Mapper;
import org.xidian.simplemapreduce.core.Partition;
import org.xidian.simplemapreduce.core.Reducer;
import org.xidian.simplemapreduce.core.impl.HashDataCollection;
import org.xidian.simplemapreduce.core.impl.HashDataReader;
import org.xidian.simplemapreduce.core.impl.HashPartition;
import org.xidian.simplemapreduce.core.impl.JobBuilder;
import org.xidian.simplemapreduce.core.impl.SortedDataCollection;
import org.xidian.simplemapreduce.core.impl.SortedDataReader;
import org.xidian.simplemapreduce.io.Writable;
import org.xidian.simplemapreduce.io.impl.TextInputFormat;
import org.xidian.simplemapreduce.io.impl.TextOutputFormat;
/**
 * 求最大值
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public class MaxValueJob {
	
	public static class MaxValueMapper implements Mapper<Long,String,Integer,Double>{

		@Override
		public void map(Long key, String value,DataCollection<Integer, Double> collection) {
			
			String[] strs = value.split("@");
			collection.collect(Integer.parseInt(strs[0]), Double.parseDouble(strs[1]));
			
		}
		
	}
	
	public static class MaxValueReducer implements Reducer<Integer,Double,Integer,Double>{

		@Override
		public void reduce(Integer key, Iterable<Double> values,Writable<Integer, Double> writer) throws IOException {
			
			double maxValue = Double.MIN_VALUE;
			
			for(double value:values){
				if(value>maxValue)
					maxValue=value;
			}
			
			writer.write(key, maxValue);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Integer,Double,Integer,Double> jobBuilder = new JobBuilder<Long,String,Integer,Double,Integer,Double>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Integer, Double>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(MaxValueMapper.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Integer>>) HashPartition.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Integer, Double>>) SortedDataCollection.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Integer, Double>>) SortedDataReader.class);
		
		jobBuilder.setReducerClass(MaxValueReducer.class);
		
		jobBuilder.createJob().excute();
	}
	
	@SuppressWarnings("unchecked")
	public static void startJobHash(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<Long,String,Integer,Double,Integer,Double> jobBuilder = new JobBuilder<Long,String,Integer,Double,Integer,Double>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(TextInputFormat.class);
		
		jobBuilder.setOutputFormatClass((Class<? extends TextOutputFormat<Integer, Double>>) TextOutputFormat.class);
		
		jobBuilder.setMapperClass(MaxValueMapper.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Integer>>) HashPartition.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Integer, Double>>) HashDataCollection.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<Integer, Double>>) HashDataReader.class);
		
		jobBuilder.setReducerClass(MaxValueReducer.class);
		
		jobBuilder.createJob().excute();
	}
	

	public static void main(String[] args) throws IOException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		startJob("E:/F/hadoopTest/a/","a.txt","max-output-0.3");
		
		//startJobHash("E:/hadoop_test/a/","a.txt","max-output-0.3");
	}
}
