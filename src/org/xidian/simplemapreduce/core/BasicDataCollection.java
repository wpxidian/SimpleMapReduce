package org.xidian.simplemapreduce.core;

import java.util.List;
import java.util.Map;

/**
 * 抽象类，用于存放map阶段从文件块中读取的数据
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 */
public abstract class BasicDataCollection<K,V> implements FrameworkDataCollection<K, V> {

	protected final Map<K,List<V>>[] datas;
	protected final Partition<K> partition;
	protected final int reducerCount;
	
	@SuppressWarnings("unchecked")
	public BasicDataCollection(Configuration configuration,Partition<K> partition){
		this.partition = partition;
		this.reducerCount = configuration.getMaxReducerCount();
		this.datas = new Map[reducerCount];
		
		initMaps();
		
	}
	/**
	 * 为Map<K,List<V>>[]数组中的每一个元素创建TreeMap对象
	 */
	protected abstract void initMaps();
	
	/**
	 * 创建一个ArrayList对象
	 * @return ArrayList对象
	 */
	protected abstract List<V> createList();
	
	
	@Override
	public final void collect(K key, V value)
	{
		int partitionId = partition.part(key,reducerCount); //由Key的哈希地址决定该节点将存储在数组中的那个位置
		
		List<V> values = datas[partitionId].get(key); 
		/**
		 * 判断数组中该位置是否有key对应的List集合存在
		 * 若存在，就将value放入List集合，然后将该key和对应的List集合作为TreeMap的结点，插入到TreeMap对象中
		 * 若不存在，创建一个ArrayList对象，将value放入List集合，然后将该key和对应的List集合作为TreeMap的结点，插入到TreeMap对象中
		 */
		if(values!=null){
			values.add(value);
		}else{
			values = createList();
			values.add(value);
			datas[partitionId].put(key, values);
		}
	}
 
	@Override
	public final Map<K,List<V>> getResult(int reducerId) {
		return datas[reducerId];
	}

}
