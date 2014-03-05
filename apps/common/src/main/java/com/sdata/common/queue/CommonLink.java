package com.sdata.common.queue;

/**
 * @author zhufb
 *
 */
public class CommonLink {

	private String link;
	private int level;
	
	public CommonLink(String link,int level){
		this.link = link;
		this.level = level;
	}
	
	public String getLink() {
		return link;
	}

	public int getLevel() {
		return level;
	}
	
	@Override
	public int hashCode(){
		return link.hashCode();
	}
	
	@Override
	public boolean equals(Object another){
		if(!(another instanceof CommonLink)){
			return false;
		}
		if(link.equals(((CommonLink)another).getLink())){
			return true;
		}
		return false;
	}
	
}
