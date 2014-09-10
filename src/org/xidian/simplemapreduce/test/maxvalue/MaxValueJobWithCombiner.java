package org.xidian.simplemapreduce.test.maxvalue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;

import org.xidian.simplemapreduce.core.Combiner;
import org.xidian.simplemapreduce.core.Configuration;
import org.xidian.simplemapreduce.core.DataCollection;
import org.xidian.simplemapreduce.core.FrameworkDataCollection;
import org.xidian.simplemapreduce.core.Mapper;
import org.xidian.simplemapreduce.core.Partition;
import org.xidian.simplemapreduce.core.Reducer;
import org.xidian.simplemapreduce.core.impl.HashPartition;
import org.xidian.simplemapreduce.core.impl.JobBuilder;
import org.xidian.simplemapreduce.core.impl.SortedDataCollection;
import org.xidian.simplemapreduce.io.Writable;
import org.xidian.simplemapreduce.io.impl.TextInputFormat;
import org.xidian.simplemapreduce.io.impl.TextOutputFormat;

public class MaxValueJobWithCombiner {
	
	public static class MaxValueMapper implements Mapper<Long,String,Integer,Double>{

		@Override
		public void map(Long key, String value,DataCollection<Integer, Double> collection) {
			
			String[] strs = value.split("@");
			collection.collect(Integer.parseInt(strs[0]), Double.parseDouble(strs[1]));
			
		}
		
	}
	
	public static class MaxValueCombiner implements Combiner<Integer, Double>{

		@Override
		public void combine(Integer key, Collection<Double> values,DataCollection<Integer, Double> outputCollection) {
			
			Iterator<Double> it = values.iterator();
			
			if(!it.hasNext()){
				return ;
			}
			
			double maxValue = it.next();
			
			for(;it.hasNext();){
				double value = it.next();
				if(maxValue<value)
					maxValue = value;
					
			}
			
			boolean isPassedMaxValue = false;
			for(it = values.iterator();it.hasNext();){
				double value = it.next();
				if(isPassedMaxValue||maxValue>value){
					it.remove();
				}else{
					isPassedMaxValue = true;
				}
					
			}
			
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
		
		jobBuilder.setDataCollectionClass((Class<? extends FrameworkDataCollection<Integer, Double>>) SortedDataCollection.class);
		
		jobBuilder.setPartitionClass((Class<? extends Partition<Integer>>) HashPartition.class);;
		
		jobBuilder.setCombinerClass(MaxValueCombiner.class);
		
		jobBuilder.setReducerClass(MaxValueReducer.class);
		
		jobBuilder.createJob().excute();
	}
	

	public static void main(String[] args) throws IOException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		startJob("E:/F/hadoopTest/a/","a.txt","/max-output-withCombiner-0.3");
	}
}
