package org.xidian.simplemapreduce.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.xidian.simplemapreduce.io.Writable;
import org.xidian.simplemapreduce.io.Readable; 
/**
 * Reducer线程类
 * @author LuJi WangPeng 
 * @version 1.0   
 * @since JDK 1.7
 */
public class BasicReducerRunner<IK, IV, OK, OV> implements Runnable {

	private final Readable<IK, List<IV>> mappedDatas; //SortedDataReader对象
	private final Reducer<IK, IV, OK, OV> reducer;
	private final Writable<OK, OV> writer;

	public BasicReducerRunner(Readable<IK, List<IV>> mappedDatas,
			Reducer<IK, IV, OK, OV> reducer, Writable<OK, OV> writer) {
		this.mappedDatas = mappedDatas;
		this.reducer = reducer;
		this.writer = writer;
	}

	@Override
	public void run() {
		try {

			while (mappedDatas.next()) {

				reducer.reduce(mappedDatas.getKey(), mappedDatas.getValue(),writer);

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			writer.close();
		}

	}

}
