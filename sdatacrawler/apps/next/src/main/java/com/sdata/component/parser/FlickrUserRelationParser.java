package com.sdata.component.parser;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.EmailUtil;

public class FlickrUserRelationParser extends SdataParser{

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrUserRelationParser");
	
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		org.dom4j.Document doc = parseXmlDocument(c);
		org.dom4j.Element rootElement = doc.getRootElement();
		Map<String,Object> metadata = new HashMap<String,Object>();
		String stat = rootElement.attributeValue("stat");
		metadata.put("stat", stat);
		if(!"ok".equals(stat)){
			org.dom4j.Element errElement = rootElement.element("err");
			String message = errElement.attributeValue("msg");
			metadata.put("message", message);
			result.setMetadata(metadata);
			return result;
		}
		org.dom4j.Element photos = rootElement.element("photos");
		if(photos==null){
			return result;
		}
		String page = photos.attributeValue("page");
		String pages = photos.attributeValue("pages");
		metadata.put("page", page);
		metadata.put("pages", pages);
		@SuppressWarnings("rawtypes")
		Iterator elementIterator = photos.elementIterator("photo");
		if(elementIterator==null || !elementIterator.hasNext()){
			return result;
		}
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		while(elementIterator.hasNext()){
			org.dom4j.Element photo = (org.dom4j.Element)elementIterator.next();
			Map<String,Object> map = new HashMap<String,Object>();
			String owner=photo.attributeValue("owner");
			String ownername=photo.attributeValue("ownername");
			map.put("key", owner);
			map.put("name", ownername);
			map.put("depth", "0");
			list.add(map);
		}
		metadata.put(Constants.USER, list);
		result.setMetadata(metadata);
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		String parseType = StringUtils.valueOf(c.getMetadata("type"));
		if(Constants.PARSER_TYPE_USER.equals(parseType)){
			ParseResult result = parserUser(c);
			return result;
		}else if(Constants.PARSER_TYPE_USER_GROUP.equals(parseType)){
			ParseResult result = parserUserGroup(c);
			return result;
		}else{
			ParseResult result = parserUserList(c);
			return result;
		}
	}

	private ParseResult parserUserList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray usersList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj;
		try {
			content = content.replace("{--", "(|-)--");
			content = content.replace("--}", "--(-|)");
			JSObj = JSONObject.fromObject(content);
		} catch (Exception e) {
			String[] emailAddress = new String[1];
			emailAddress[0] = "qiumm820@gmail.com";
			EmailUtil.send(emailAddress, "Flickr formatObject Exception", content);
			throw new NegligibleException(e);
		}
		JSONObject contactsObj = (JSONObject)JSObj.get("contacts");
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("stat", stat);
		prMetadata.put("message", message);
		if(contactsObj!=null){
			String page = StringUtils.valueOf(contactsObj.get("page"));
			String pages = StringUtils.valueOf(contactsObj.get("pages"));
			prMetadata.put("page", page);
			prMetadata.put("pages", pages);
			JSONArray contactArr = (JSONArray)contactsObj.get("contact");
			if(contactArr!=null){
				Iterator<JSONObject> contactIterator= contactArr.iterator();
				while(contactIterator.hasNext()){
					JSONObject contObj = contactIterator.next();
					Object nsid = contObj.get("nsid");
					if(nsid==null || StringUtils.isEmpty(nsid.toString())){
						//log.warn("user name is empty:"+infoObj.toString());
						continue;
					}
					usersList.add(contObj);
				}
			}
		}
		prMetadata.put(Constants.FLICKR_CONTACT, usersList);
		result.setMetadata(prMetadata);
		return result;
	}

	private ParseResult parserUser(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		String content = c.getContent();
		JSONObject JSObj;
		try {
			JSObj = JSONObject.fromObject(content);
		} catch (Exception e) {
			throw new NegligibleException(e);
		}
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		JSONObject userinfo = (JSONObject)JSObj.get("person");
		if(userinfo!=null){
			String id =StringUtils.valueOf(userinfo.get("id")) ;
			String _id = id.replace("@", "0").replace("N", "1");
			userinfo.put(Constants.OBJECT_ID, Long.parseLong(_id));
			//deal the userinfo for example:
			//		"username": { "_content": "houdejun214" } deal to "username":"houdejun214", 
			JSONObject username = (JSONObject)userinfo.get("username");
			String _content =StringUtils.valueOf(username.get("_content")) ;
			userinfo.put("username", _content);
			if(userinfo.containsKey("realname")){//some people don't has this property
				JSONObject realname = (JSONObject)userinfo.get("realname");
				_content =StringUtils.valueOf(realname.get("_content")) ;
				userinfo.put("realname", _content);
			}
			if(userinfo.containsKey("location")){
				JSONObject location = (JSONObject)userinfo.get("location");
				_content =StringUtils.valueOf(location.get("_content")) ;
				userinfo.put("location", _content);
			}
			JSONObject description = (JSONObject)userinfo.get("description");
			_content =StringUtils.valueOf(description.get("_content")) ;
			userinfo.put("description", _content);
			JSONObject photosurl = (JSONObject)userinfo.get("photosurl");
			_content =StringUtils.valueOf(photosurl.get("_content")) ;
			userinfo.put("photosurl", _content);
			JSONObject profileurl = (JSONObject)userinfo.get("profileurl");
			_content =StringUtils.valueOf(profileurl.get("_content")) ;
			userinfo.put("profileurl", _content);
			JSONObject mobileurl = (JSONObject)userinfo.get("mobileurl");
			_content =StringUtils.valueOf(mobileurl.get("_content")) ;
			userinfo.put("mobileurl", _content);
			
			userinfo.put("stat", stat);
			userinfo.put("message", message);
			Map<String, JSONObject> metadata = new HashMap<String,JSONObject>();
			metadata.put(Constants.USER, userinfo);
			result.setMetadata(metadata);
		}else{
			userinfo = new JSONObject();
			userinfo.put("stat", stat);
			userinfo.put("message", message);
			Map<String, JSONObject> metadata = new HashMap<String,JSONObject>();
			metadata.put(Constants.USER, userinfo);
			result.setMetadata(metadata);
		}
		return result;
	}
	
	private ParseResult parserUserGroup(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray groupList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj;
		try {
			JSObj = JSONObject.fromObject(content);
		} catch (Exception e) {
			throw new NegligibleException(e);
		}
		JSONObject contactsObj = (JSONObject)JSObj.get("groups");
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("stat", stat);
		prMetadata.put("message", message);
		
		if(contactsObj!=null){
			JSONArray contactArr = (JSONArray)contactsObj.get("group");
			if(contactArr!=null){
				Iterator<JSONObject> contactIterator= contactArr.iterator();
				while(contactIterator.hasNext()){
					JSONObject contObj = contactIterator.next();
					String nsid =(String)contObj.get("nsid");
//					nsid = nsid.replace("@", "0").replace("N", "1");
					if(nsid==null || StringUtils.isEmpty(nsid.toString())){
						//log.warn("user name is empty:"+infoObj.toString());
						continue;
					}
					groupList.add(nsid);
				}
			}
		}
		prMetadata.put(Constants.FLICKR_GROUPLIST, groupList);
		result.setMetadata(prMetadata);
		return result;
	}
	
	public FlickrUserRelationParser(Configuration conf,RunState state){
		setConf(conf);
	}
	
	public static void main(String[] args){
		String path = "D:/30.txt";
		String resourceUrl = ApplicationResourceUtils.getResourceUrl(path);
		File file = new File(resourceUrl);
		String content = null;
		try {
			content = FileUtils.readFileToString(file,  "utf-8");
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		JSONObject JSObj;
		try {
			content = content.replace("{--", "(|-)--");
			content = content.replace("--}", "--(-|)");
			JSObj = JSONObject.fromObject(content);
		} catch (Exception e) {
			System.out.println(content);
			throw new NegligibleException(e);
		}
		System.out.println("fromObject is ok.");
	}
	
}
