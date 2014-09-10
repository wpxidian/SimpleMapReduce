package org.xidian.simplemapreduce.test.wordcount;

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
import org.xidian.simplemapreduce.io.Writable;
import org.xidian.simplemapreduce.test.wordcount.MultiLineFile;

public class WordCountJob {
	
	public static class WordCountMapper implements Mapper<String,Integer,String,Integer>{

		@Override
		public void map(String key, Integer value,
				DataCollection<String, Integer> collection) {
			collection.collect(key, value);
		}
		
	}
	
	public static class WordCountReducer implements Reducer<String,Integer,String,Integer>{

		@Override
		public void reduce(String key, Iterable<Integer> values,Writable<String, Integer> writer) throws IOException {
			Integer sum = 0 ;
			for(Integer i:values){
				sum += i ;
			}
			writer.write(key, sum);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public static void startJob(String inputPath,String fileName,String outputPath) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		
		final String path = inputPath+fileName;
		
		Configuration configuration = new Configuration();
		
		JobBuilder<String,Integer,String,Integer,String,Integer> jobBuilder = new JobBuilder<String,Integer,String,Integer,String,Integer>(configuration);
		
		jobBuilder.setInputPath(path);
		
		jobBuilder.setOutputPath(inputPath+outputPath);
		
		jobBuilder.setInputFormatClass(MultiLineFile.class);
		
		jobBuilder.setOutputFormatClass(MultiLineFile.class);
		
		jobBuilder.setMapperClass(WordCountMapper.class);
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<String, Integer>>) HashDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<String>>) HashPartition.class);
		
		jobBuilder.setDataReaderClass((Class<? extends BasicDataReader<String, Integer>>) HashDataReader.class);
		
		jobBuilder.setReducerClass((Class<? extends Reducer<String, Integer, String, Integer>>) WordCountReducer.class);
		
		jobBuilder.createJob().excute();
	}
	
	public static void main(String[] args) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException, InterruptedException {
		
		startJob("/b/","b","word");
		
	}
}
