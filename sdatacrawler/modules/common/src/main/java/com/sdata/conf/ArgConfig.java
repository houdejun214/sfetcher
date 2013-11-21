package com.sdata.conf;

import java.util.HashMap;

import com.lakeside.core.utils.StringUtils;

/**
 * 
 * represent the argument configure setting
 * 
 * @author houdejun
 *
 */
public class ArgConfig extends HashMap<String, String>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ArgConfig(String[] args,	int argLength){
		for( int i=0;i<argLength;i++){
			String arg = args[i];
			if(arg.startsWith("--")) {
				arg = StringUtils.chompHeader(arg, "--");
				if(arg.indexOf("=")>-1){
					String[] splits = arg.split("=");
					if(splits!=null && splits.length==2){
						this.put(splits[0], splits[1]);
					}
				}else{
					this.put(arg,"true");
				}
			}
		}
	}
	
	/**
	 * get value from keys, keys can be array list.
	 * the first key that contained in the map will be return.
	 * @param names
	 * @return
	 */
	public String getValue(String... names){
		for(int i=0;i<names.length;i++){
			if(this.containsKey(names[i])){
				return this.get(names[i]);
			}
		}
		return null;
		
	}
	
	public boolean haveArg(String name){
		if(this.containsKey(name)){
			return true;
		}
		return false;
	}
}