package com.sdata.common;

import com.sdata.proxy.SenseFetchDatum;

/**
 * @author zhufb
 *
 */
public class CommonDatum extends SenseFetchDatum{
	
	private int level;

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}
	
	@Override
	public boolean valid(){
//		if(!super.valid()){
//			return false;
//		}
		// add level limit when current level >= level limit return false
		CommonItem item = (CommonItem)this.getCrawlItem();
		// when get datum level > level limit end
		if(level > item.getLevelLimit()){
			return false;
		}
		return true;
	}
	
}
