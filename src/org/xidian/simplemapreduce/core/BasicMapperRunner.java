package org.xidian.simplemapreduce.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.xidian.simplemapreduce.io.Readable; 
/**
 * 执行Map任务的线程类  
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public class BasicMapperRunner<IK,IV,OK,OV> implements Runnable{
	
	private final Mapper<IK,IV,OK,OV> mapper;
	private final FrameworkDataCollection<OK,OV> outCollecton;
	private final Combiner<OK,OV> combiner;
	private final int reducerCount;
	private final BlockingQueue<Readable<IK, IV>> workingQueue;
	private final Readable<IK,IV> deadPill;
	
	public BasicMapperRunner(Mapper<IK,IV,OK,OV> mapper,FrameworkDataCollection<OK,OV> outCollecton,Combiner<OK, OV> combiner,Configuration configuration,BlockingQueue<Readable<IK, IV>> workingQueue,Readable<IK,IV> deadPill)
	{
		this.mapper = mapper;
		this.outCollecton = outCollecton;
		this.combiner = combiner;
		this.reducerCount = configuration.getMaxReducerCount();
		this.workingQueue = workingQueue;
		this.deadPill = deadPill;
	}
	
	@Override
	public void run() {
		try {
			Readable<IK, IV> reader;
			
			while(true){

				reader = workingQueue.take(); //从队头取出一个元素，如果队列为空，则一直阻塞
				
				if(reader!=deadPill){
					while(reader.next()) //从分块文件中读取一行数据，此时行数key加一，value为读出的数据
						/**
						 * key是行号，value是每行的内容,然后调用SortMapper的map方法，将Value中的值(97@0.5023462974213119)拆分，封装成Data对象
						 * 然后map再调用SortedDataCollection的collect方法，将Data对象作为Key，Nothing的一个实例作为value，插入到SortedDataCollection中
						 * SortedDataCollection中有一个Map<K,List<V>>[]数组，数组大小是reducerCount的大小
						 * 数组中的元素是一个TreeMap的实例，可对元素进行排序，因为Data对象实现了Comparable接口
						 * 有多少个MapperRunner对象就对应有多少个Map<K,List<V>>[]数组
						 * 将这些数组放到一个List集合（mapOutputCollections）中，交由reduce阶段处理
						 */
						mapper.map(reader.getKey(), reader.getValue(), outCollecton);
				}else{
					workingQueue.put(deadPill); //若读出的是最后处理结束的哨兵，则把哨兵再放回到队列中,？？？
					break;
				}
				
			}
			
			if(combiner!=null){
				
				for(int i=0;i<reducerCount;i++){
					
					Map<OK,List<OV>> map = outCollecton.getResult(i);
					
					for(OK key:map.keySet()){
						combiner.combine(key, map.get(key), outCollecton);
					}
				}
				
			}
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
	}
	
}
