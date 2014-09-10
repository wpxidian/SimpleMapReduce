package org.xidian.simplemapreduce.io.data;

public class Nothing {
	
	public static final Nothing INSTANCE = new Nothing();
	
	private static final String name = "";
	
	private Nothing(){
		
	}

	@Override
	public String toString() {
		return name;
	}
	
}
