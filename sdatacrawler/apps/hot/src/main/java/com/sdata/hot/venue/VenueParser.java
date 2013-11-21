package com.sdata.hot.venue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.hot.Hot;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class VenueParser extends SdataParser{
	
	private int count;
	private FoursquareApi api;
	
	public VenueParser(Configuration conf){
		this.count = conf.getInt("crawl.count", 3);
		this.api = new FoursquareApi(conf);
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		this.addTrends(result);
		this.add(Hot.Food, result);
		this.add(Hot.Shops, result);
		return result;
	}
	
	private void addTrends(ParseResult result){
		//trends
		JSONArray venues = null;
		while(venues==null){
			venues = api.getTrendingVenues(count*2);
		}
		Date fetTime = new Date();
		List<String> list = new ArrayList<String>();
		for(int i=1;i<=venues.size()&&list.size()<count;i++){
			JSONObject v = (JSONObject)venues.get(i-1);
			String id = v.getString("id");
			if(list.contains(id)){
				continue;
			}
			list.add(id);
			result.addFetchDatum(this.getVenue(Hot.Venue,list.size(), id,fetTime));
		}
	}
	
	/**
	 * @param cate
	 * @param result
	 */
	private void add(Hot hot,ParseResult result){
		int offset = 0;
		int limit = 50;
		String cate = Hot.Food.equals(hot)?"food":"shops";
		List<Venue> list = new ArrayList<Venue>();
		ComparatorVenue c = new ComparatorVenue();
		while(true){
			List foods = api.getExploreVenues(cate, offset,limit);
			if(foods==null||foods.size()==0){
				break;
			}
			foods = ((List)foods.get(0));
			Iterator iterator = foods.iterator();
			while(iterator.hasNext()){
				JSONObject v = (JSONObject)iterator.next();
				String id = v.getString("id");
				String strHN =StringUtils.valueOf(MapUtils.getInter(v, "hereNow/count"));
				if(StringUtils.isEmpty(id)||!StringUtils.isNum(strHN)){
					continue;
				}
				Integer hn = Integer.valueOf(strHN);
				if(list.size()<count){
					list.add(new Venue(id,hn));
					Collections.sort(list, c);
					continue;
				}
				Venue lastV = list.get(count-1);
				if(lastV.getHerenow()>=hn){
					continue;
				}
				if(containVenue(list,id)){
					continue;
				}
				list.remove(count-1);
				list.add(new Venue(id,hn));
				Collections.sort(list, c);
			}
			//当返回的venue数量少于limit时，已经是最后了
			if(foods.size()<limit){
				break;
			}
			offset+=limit;
		}
		Date fetTime = new Date();
		for(int i=1;i<=list.size();i++){
			result.addFetchDatum(this.getVenue(hot,i,list.get(i-1).getId(),fetTime));
		}
	}
	
	private boolean containVenue(List<Venue> list,String id){
		for(Venue v:list){
			if(v.getId().equals(id)){
				return  true;
			}
		}
		return false;
	}

	private FetchDatum getVenue(Hot hot,int rank,String id,Date fetTime){
		FetchDatum datum = new FetchDatum();
		JSONObject venue = api.getVenue(id);
		byte[] rk = HotUtils.getRowkey(hot.getValue(), fetTime, rank);
		datum.addMetadata("rk", rk);
		datum.addMetadata("rank", rank);
		datum.addMetadata("type", hot.getValue());
		datum.addMetadata("fet_time", fetTime);
		datum.addMetadata("pub_time", fetTime);
		datum.addMetadata("id", id);
		datum.addMetadata("title", venue.get("name"));
		datum.addMetadata("content", venue.get("description"));
		datum.addMetadata("target", venue.get("canonicalUrl"));
		JSONArray groups = venue.getJSONObject("photos").getJSONArray("groups");
		Iterator iterator = groups.iterator();
		while(iterator.hasNext()){
			JSONObject next = (JSONObject)iterator.next();
			if("venue".equals(next.get("type"))){
				JSONObject photo = (JSONObject) next.getJSONArray("items").get(0);
				datum.addMetadata("image", photo.getString("url"));
				break;
			}
		}
		datum.addMetadata("address", MapUtils.getInter(venue,"location/address"));
		datum.addMetadata("geo", MapUtils.getInter(venue,"location/lat")+","+MapUtils.getInter(venue, "location/lng"));
		datum.addMetadata("checkins", MapUtils.getInter(venue,"stats/checkinsCount"));
		datum.addMetadata("herenow", MapUtils.getInter(venue,"hereNow/count"));
		datum.setUrl(venue.getString("canonicalUrl"));
		return datum;
	}

	class ComparatorVenue implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			Integer h0 = ((Venue)arg0).getHerenow();
			Integer h1 = ((Venue)arg1).getHerenow();
			return h1.compareTo(h0);
		 }
	}

	class Venue{
		private String id;
		private int herenow;
		public Venue(String id,int herenow){
			this.id = id;
			this.herenow = herenow;
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public int getHerenow() {
			return herenow;
		}
		public void setHerenow(int herenow) {
			this.herenow = herenow;
		}
		
	}
}
