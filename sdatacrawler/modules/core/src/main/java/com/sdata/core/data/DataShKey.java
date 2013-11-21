package com.sdata.core.data;


/**
 * @author zhufb
 *
 */
public class DataShKey {

	private  int HIGH_DIGIT = 6;
	private  int LOW_DIGIT = 3;
	
	public DataShKey(){
		
	}

	public DataShKey(int high){
		this.HIGH_DIGIT = high;
	}

	public String getShKey(String data){
		return getHighStr(data).concat(getLowStr(data));
	}
	
	private String getLowStr(String id){
		StringBuffer result = new StringBuffer();
		if(id.length()<LOW_DIGIT){
			for(int i=0;i<LOW_DIGIT -id.length();i++){
				result.append(0);
			}
			return result.append(id).toString();
		}
		return id.substring(id.length()-LOW_DIGIT, id.length());
	}
	
	private String getHighStr(String id){
		if(id.length()<=LOW_DIGIT){
			return "";
		}
		if(id.length()-LOW_DIGIT<HIGH_DIGIT){
			return id.substring(0,id.length()-LOW_DIGIT);
		}
		return id.substring(0,HIGH_DIGIT);
	}
}
