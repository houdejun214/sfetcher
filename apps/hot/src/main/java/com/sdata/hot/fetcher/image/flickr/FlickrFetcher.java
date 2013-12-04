package com.sdata.hot.fetcher.image.flickr;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.hot.HotConstants;
import com.sdata.hot.Source;
import com.sdata.hot.fetcher.image.HotImageFetcher;

/**
 * @author zhufb
 *
 */
public class FlickrFetcher extends HotImageFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.FlickrFetcher");
	private int count;
	private FlickrApi api;
	private String FLICKR_PREFIX = "http://www.flickr.com/photos/";
	private String Flickr_Head = "http://farm{0}.staticflickr.com/{1}/buddyicons/{2}.jpg";
	
	public FlickrFetcher() {
		
	}
	
	public FlickrFetcher(Configuration conf) {
		super(conf);
		this.count = conf.getInt("crawl.count", 3);
		this.api = new FlickrApi(conf);
	}
	
	@Override
	public List<FetchDatum> getDatumList() {
		Date date = DateTimeUtils.getBeginOfDay(new Date());
		long unixTime = DateTimeUtils.getUnixTime(date);
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		JSONObject JSObj = api.getPopular(unixTime / 1000, count);
		if (JSObj.isNullObject()) {
			return result;
		}
		String stat = JSObj.getString("stat");
		if (!"ok".equals(stat)) {
			return result;
		}
		JSONArray mediaData = (JSONArray) JSObj.getJSONObject("photos").get(
				"photo");
		if (mediaData == null || mediaData.size() == 0) {
			return result;
		}

		for (int i = 1; i <= mediaData.size(); i++) {
			FetchDatum datum = new FetchDatum();
			JSONObject json = mediaData.getJSONObject(i - 1);
			Object pubTime = DateFormat.changeStrToDate(json.get("dateupload"));
			datum.addMetadata(HotConstants.PUBLIC_TIME, pubTime);
			JSONObject caption = json.getJSONObject("description");
			if (!caption.isNullObject()
					&& !StringUtils.isEmpty(caption.getString("_content"))) {
				datum.addMetadata("content", caption.get("_content"));
			}
			StringBuffer target = new StringBuffer(FLICKR_PREFIX);
			Object image = json.get("url_l");
			if(image==null||StringUtils.isEmpty(image.toString())){
				image = json.get("url_z");
			}
			datum.addMetadata("image", image);
			datum.addMetadata("uname", json.get("ownername"));
			Object uid = json.get("owner");
			Object id = json.get("id");
			datum.addMetadata("uid", uid);
			datum.addMetadata("title", json.get("title"));
			target.append(uid).append("/");
			target.append(id);

			datum.addMetadata("views", json.get("views"));
			Object updateTime = DateFormat.changeStrToDate(json
					.get("lastupdate"));
			datum.addMetadata("update_time", updateTime);
			Object takenTime = DateFormat
					.changeStrToDate(json.get("datetaken"));
			datum.addMetadata("taken_time", takenTime);

			if (json.containsKey("latitude") && json.containsKey("longitude")) {
				datum.addMetadata("geo",
						json.get("latitude") + "," + json.get("longitude"));
			}
			byte[] rk = getHotRowKeyBytes(i);
			datum.addMetadata(HotConstants.ROWKEY, rk);
			datum.addMetadata(HotConstants.RANK, i);
			datum.addMetadata(HotConstants.BATCH_TIME, super.getBatchTime());
			datum.addMetadata(HotConstants.ID, id);
			datum.addMetadata(HotConstants.TYPE, type().getValue());// type
			datum.addMetadata(HotConstants.SOURCE, source().getValue());// source
			datum.addMetadata(HotConstants.FETCH_TIME, new Date());
			datum.addMetadata("target", target.toString());
			datum.addMetadata("link", image);
			datum.setId(id);
			datum.setUrl(target.toString());
			result.add(datum);
		}
		return result;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		JSONObject photo = this.api.getPhoto(Long.valueOf(StringUtils.valueOf(datum.getId())));
		Object farm = MapUtils.getInter(photo, "photo/owner/iconfarm");
		Object server = MapUtils.getInter(photo, "photo/owner/iconserver");
		Object nsid = MapUtils.getInter(photo, "photo/owner/nsid");
		String head = MessageFormat.format(Flickr_Head, farm,server,nsid);
		datum.addMetadata("head", head);
		datum.addMetadata("comments", MapUtils.getInter(photo, "photo/comments/_content"));
		return datum;
	}
	
	public Source source(){
		return Source.Flickr;
	}

}
