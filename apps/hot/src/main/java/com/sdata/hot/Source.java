package com.sdata.hot;

/**
 * @author zhufb
 *
 */
public enum Source {
	Instagram("instagram"),
	Foursquare("foursquare"),
	Google("google"),
	Stomp("stomp"),
	Twitter("twitter"),
	Flickr("flickr"),
	Youtube("youtube");
	
	private String name;
	private Source(String name){
		this.name = name;
	}
	public String getValue(){
		return name;
	}
	
}
