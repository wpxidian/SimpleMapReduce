package org.xidian.simplemapreduce.io.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.xidian.simplemapreduce.io.InputFormat;
import org.xidian.simplemapreduce.io.Readable;
import org.xidian.simplemapreduce.util.PropertiesConfigurationReader;

/**
 * 这是一个 {@code InputFormat}的实现类，用于处理单个文本，该类会将文本按行分割，并读取configuration中的
 * 最大分片值，按照该值建立分片并返回每个分片的Readable对象。
 * 
 * @param <T> 这是一个<code>Readable<K,V></code>接口的子类型
 * @param <K> 可为任意确定对象类型的键，作为<code>Readable<K,V></code>的键(K)
 * @param <V> 可为任意确定对象类型的值，作为<code>Readable<K,V></code>的值(V)
 * 
 * @author WangPeng 
 * @version 1.0   
 * @since JDK 1.8
 * 
 */
public class TextInputFormat extends InputFormat<Long, String> {
	
	private final File file; //输入的文件
	private final int bufferSize; //缓冲的大小
	private final Collection<TextReader> textReaders;//存放文件分块后，每块文件对应一个阅读器，textReaders用来存放阅读器的集合
	private final int maxSplitoCount; //最大的分快数
	
	/**
	 * 构造器，用于初始化
	 * @param path
	 * @throws IOException
	 */
	public TextInputFormat(String path) throws IOException {
		
		super(path);
		
		this.file = new File(path);
		
		PropertiesConfigurationReader pcr = new PropertiesConfigurationReader(TextInputFormat.class, "textinputformat.properties");
		
		bufferSize = pcr.getInt("bufferSize");
		
		maxSplitoCount = pcr.getInt("maxSplitoCount");
		
		this.textReaders = new ArrayList<TextReader>(maxSplitoCount);
		
		final List<Long> perOffset = getEachSplitoOffset();
		
		for(int i=0;i<perOffset.size()-1;i++){
			TextReader textReader = new TextReader(file, perOffset.get(i), perOffset.get(i+1), bufferSize);
			textReaders.add(textReader);
		}
		
	}
	/**
	 * 将文件划分成大小相等的N块
	 * @return 返回每个文件块在文件中的起始位置的集合（LinkedList对象）
	 * @throws IOException
	 */
	private List<Long> getEachSplitoOffset() throws IOException{

		long perSplitpSize = file.length()/maxSplitoCount +1; //每块的大小=文件大小/分块个数
		
		final List<Long> perOffset = new LinkedList<Long>(); //存放文件分块后，每块的起始位置
		
		long currentOffset = 0;
		
		perOffset.add(0L);//第一块的起始位置是0
		
		RandomAccessFile raf = new RandomAccessFile(file, "r");//随机文件访问类
		
		try{
			
			for(long i = perSplitpSize;i<file.length();i += perSplitpSize)
			{
	
				 raf.seek(i);
				 
				 raf.readLine(); //保证读取的内容是整行
				 
				 currentOffset = raf.getFilePointer(); //当前光标的位置
				 
				 raf.seek(currentOffset);
	
				 perOffset.add(currentOffset);
				 
				 while(currentOffset>i + perSplitpSize)
					 i += perSplitpSize;
			}
			
		}finally{
			raf.close();
		}
		
		perOffset.add(file.length()); //文件的末尾
		
		return perOffset;
	}
	/**
	 * 静态内部类，用于读取每个文本块的内容
	 * @author WangPeng 
	 * @version 1.0   
	 * @since JDK 1.7
	 */
	private static class TextReader implements Readable<Long, String>{
		
		private final long endOffset;
		
		private final BufferedRandomAccessFileReader brafr;
		
		//line number用key存储行号
		private long key = 0;
		
		//value用来存储每行的内容
		private String value = null;
		
		private TextReader(File file,long startOffset,long endOffset,int bufferSize) throws IOException{
			this.endOffset = endOffset;
			brafr = new BufferedRandomAccessFileReader(file, startOffset, bufferSize);
		}
		
		//判断文件块是否已经读完
		@Override
		public boolean next() throws IOException {
			try{
			if(brafr.getFilePointer()<endOffset){
				key++;
				value = brafr.readLine();
				return true;
			}
			brafr.close();
			return false;
			}catch(IOException e){
				brafr.close();
				throw e;
			}
		}

		@Override
		public Long getKey() {
			return key;
		}

		@Override
		public String getValue() {
			return value;
		}
		
	}

	@Override
	public int getFragmentCount() {
		return textReaders.size();
	}

	@Override
	public Collection<TextReader> getReaders() {
		return textReaders;
	}


}
