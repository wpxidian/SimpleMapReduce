package org.xidian.simplemapreduce.test.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xidian.simplemapreduce.io.Readable;
import org.xidian.simplemapreduce.util.PropertiesConfigurationReader;

public class MultiLineFile extends MultiFile<String,Integer>{
	
	private static final int bufferSize = getBufferSize();
	private static final char lineToken = getLineToken();
	private static LinkedList<String> keys = new LinkedList<>() ;
	
	private File currentBlockItem;
	private long currentBlockSize;
	private PrintWriter pw;
	private StringBuilder sb;
	
	private static int getBufferSize(){
		try {
			PropertiesConfigurationReader reader = new PropertiesConfigurationReader(MultiLineFile.class, "multidisklinefile.properties");
			return reader.getInt("bufferSize");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static char getLineToken(){
		try {
			PropertiesConfigurationReader reader = new PropertiesConfigurationReader(MultiLineFile.class, "multidisklinefile.properties");
			return reader.getChar("lineToken");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	

	public MultiLineFile(String path)throws IOException {
		super(path, true);
		
		if(!blockItems.isEmpty()){
			currentBlockItem = blockItems.get(blockItems.size());
			currentBlockSize = currentBlockItem.length();
		}
		
		sb = new StringBuilder();
	}
	
	public MultiFile<String,String>[] listChild() {
		throw new UnsupportedOperationException("MultiDiskLineFile类不支持listChildPath方法，因为该类都指向某一个文件，所以没有子文件.");
	}
	
	@Override
	public void close() {
		if(pw!=null)
			pw.close();
	}
	
	public void write(String key, Integer value) throws IOException {
		
		if(currentBlockItem!=null&&currentBlockSize<BLOCKSIZE){
			
			if(pw==null)
				pw = new PrintWriter(currentBlockItem);
			
		}else{
			if(!isExists)
				createFileORDir();
			
			if(pw!=null)
				pw.close();
			
			int fileId = blockItems.size()+1;
			
			int folderNum = (fileId-1)%folderItems.size();
			
			currentBlockItem = new File(sb.append(folderItems.get(folderNum).getPath()).append(File.separator).append(fileId).toString());
			currentBlockSize = 0l;
			logger.debug("create new block"+sb.toString());
			
			sb.delete(0, sb.length());
			
			currentBlockItem.createNewFile();
			
			pw = new PrintWriter(currentBlockItem);
			
			blockItems.put(fileId, currentBlockItem);
			
		}
		if(key != null && !"".equals(key.trim())){
			pw.println(key + lineToken + (value==null?"":value));
			currentBlockSize += key.length();
		}
	}

	@Override
	public Collection<? extends Readable<String, Integer>> getReaders() {
		
		List<LineReader> lineReaders = new ArrayList<LineReader>(getFragmentCount());
		try {
			for(Entry<Integer, File> entry: blockItems.entrySet()){
					lineReaders.add(new LineReader(entry.getValue()));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return lineReaders;
	}
	
	private static class LineReader implements Readable<String, Integer>{
		
		private final BufferedReader bufferReader;
		
		private String key = null;
		
		private Integer value = null;
		
		private LineReader(File file) throws IOException{
			bufferReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)),bufferSize);
		}
		
		@Override
		public boolean next() throws IOException {
			
			key = keys.pollFirst() ;
			
			while(key == null){
				if(keys.isEmpty()){
					String str = bufferReader.readLine();
					if(str==null)
						return false ;
					String s = "\\w+" ;
					Pattern pattern = Pattern.compile(s) ;
					Matcher ma = pattern.matcher(str) ;
					while(ma.find()){
						keys.add(ma.group()) ;
					}
				}
				key = keys.pollFirst() ;
			}
			value = 1 ;
			System.out.println(key);
			return true;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public Integer getValue() {
			return value;
		}
		
	}
}
