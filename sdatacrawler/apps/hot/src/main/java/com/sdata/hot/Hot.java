package com.sdata.hot;

/**
 * @author zhufb
 *
 */
public enum Hot {
	
	All(0,"all"),
	Event(1,"event"),
	Interest(2,"interest"),
	Venue(3,"venue"),
	Image(4,"image"),
	Social(5,"social"),
	Food(6,"food"),
	Shops(7,"shops"),
	Video(8,"video");
	
	private Integer type;
	private String name;
	private Hot(int type,String name){
		this.type = type;
		this.name = name;
	}
	
	public Integer getValue(){
		return type;
	}

	public String getName(){
		return name;
	}
	
	public static Hot get(int type){
		if(All.getValue() == type){
			return All;
		}else if(Event.getValue() == type){
			return Event;
		}else if(Interest.getValue() == type){
			return Interest;
		}else if(Venue.getValue() == type){
			return Venue;
		}else if(Image.getValue() == type){
			return Image;
		}else if(Social.getValue() == type){
			return Social;
		}else if(Food.getValue() == type){
			return Food;
		}else if(Shops.getValue() == type){
			return Shops;
		}else if(Video.getValue() == type){
			return Video;
		}
		return All;
	}
	
	public static Hot get(String name){
		if(All.getName().equals(name)){
			return All;
		}else if(Event.getName().equals(name)){
			return Event;
		}else if(Interest.getName().equals(name)){
			return Interest;
		}else if(Venue.getName().equals(name)){
			return Venue;
		}else if(Image.getName().equals(name)){
			return Image;
		}else if(Social.getName().equals(name)){
			return Social;
		}else if(Food.getName().equals(name)){
			return Food;
		}else if(Shops.getName().equals(name)){
			return Shops;
		}else if(Video.getName().equals(name)){
			return Video;
		}
		return All;
	}
	
	public String toString(){
		return "type:"+type+",name:"+name;
	}
}
