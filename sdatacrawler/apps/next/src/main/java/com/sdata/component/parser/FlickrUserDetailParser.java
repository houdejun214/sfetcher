package com.sdata.component.parser;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;

public class FlickrUserDetailParser extends SdataParser{

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrUserRelationParser");
	
	@Override
	public ParseResult parseList(RawContent c) {
		String parseType = StringUtils.valueOf(c.getMetadata("type"));
		if(Constants.PARSER_TYPE_USER.equals(parseType)){
			ParseResult result = parserUser(c);
			return result;
		}else if(Constants.PARSER_TYPE_IMAGELIST.equals(parseType)){
			ParseResult result = parserImageList(c);
			return result;
		}else if(Constants.PARSER_TYPE_GROUPLIST.equals(parseType)){
			ParseResult result = parserGroupList(c);
			return result;
		}else if(Constants.PARSER_TYPE_MEMBERLIST.equals(parseType)){
			ParseResult result = parserMemberList(c);
			return result;
		}else{
			ParseResult result = parserUserList(c);
			return result;
		}
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		if (c.isEmpty()) {
			log.warn("fetch content is empty!");
			return null;
		}
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		JSONObject groupinfo = (JSONObject) JSObj.get("group");
		if (groupinfo != null) {
			String id = StringUtils.valueOf(groupinfo.get("id"));
			String _id = id.replace("@", "0").replace("N", "1");
			groupinfo.put(Constants.OBJECT_ID, Long.parseLong(_id));
			// deal the userinfo for example:
			// "username": { "_content": "houdejun214" } deal to
			// "username":"houdejun214",
			if (groupinfo.containsKey("name")) {
				JSONObject name = (JSONObject) groupinfo.get("name");
				String _content = StringUtils.valueOf(name.get("_content"));
				groupinfo.put("name", name);
			}
			if (groupinfo.containsKey("description")) {
				JSONObject description = (JSONObject) groupinfo
						.get("description");
				String _content = StringUtils.valueOf(description
						.get("_content"));
				groupinfo.put("description", _content);
			}
			if (groupinfo.containsKey("members")) {
				JSONObject description = (JSONObject) groupinfo.get("members");
				String _content = StringUtils.valueOf(description
						.get("_content"));
				groupinfo.put("members", _content);
			}
			if (groupinfo.containsKey("privacy")) {
				JSONObject description = (JSONObject) groupinfo.get("privacy");
				String _content = StringUtils.valueOf(description
						.get("_content"));
				groupinfo.put("privacy", _content);
			}
			groupinfo.put("stat", stat);
			groupinfo.put("message", message);
			Map<String, JSONObject> metadata = new HashMap<String, JSONObject>();
			metadata.put(Constants.FLICKR_GROUP, groupinfo);
			result.setMetadata(metadata);
		} else {
			groupinfo = new JSONObject();
			groupinfo.put("stat", stat);
			groupinfo.put("message", message);
			Map<String, JSONObject> metadata = new HashMap<String, JSONObject>();
			metadata.put(Constants.FLICKR_GROUP, groupinfo);
			result.setMetadata(metadata);
		}
		return result;
	}
	
	private ParseResult parserMemberList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray usersList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		JSONObject membersObj = (JSONObject)JSObj.get("members");
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("stat", stat);
		prMetadata.put("message", message);
		if(membersObj!=null){
			String page = StringUtils.valueOf(membersObj.get("page"));
			String pages = StringUtils.valueOf(membersObj.get("pages"));
			prMetadata.put("page", page);
			prMetadata.put("pages", pages);
			JSONArray memberArr = (JSONArray)membersObj.get("member");
			if(memberArr!=null){
				Iterator<JSONObject> memberIterator= memberArr.iterator();
				while(memberIterator.hasNext()){
					JSONObject contObj = memberIterator.next();
					Object nsid = contObj.get("nsid");
					if(nsid==null || StringUtils.isEmpty(nsid.toString())){
						//log.warn("user name is empty:"+infoObj.toString());
						continue;
					}
					usersList.add(contObj);
				}
			}
		}
		prMetadata.put(Constants.FLICKR_MEMBERLIST, usersList);
		result.setMetadata(prMetadata);
		return result;
	}
	
	private ParseResult parserGroupList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray usersList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		JSONObject groupsObj = (JSONObject)JSObj.get("groups");
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("stat", stat);
		prMetadata.put("message", message);
		if(groupsObj!=null){
			JSONArray groupArr = (JSONArray)groupsObj.get("group");
			if(groupArr!=null){
				Iterator<JSONObject> groupIterator= groupArr.iterator();
				while(groupIterator.hasNext()){
					JSONObject contObj = groupIterator.next();
					Object nsid = contObj.get("nsid");
					if(nsid==null || StringUtils.isEmpty(nsid.toString())){
						//log.warn("user name is empty:"+infoObj.toString());
						continue;
					}
					usersList.add(contObj);
				}
			}
		}
		prMetadata.put(Constants.FLICKR_GROUPLIST, usersList);
		result.setMetadata(prMetadata);
		return result;
	}
	
	private ParseResult parserImageList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray imagesList = new JSONArray();
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONObject JSObj;
		try {
			JSObj = JSONObject.fromObject(content);
		} catch (Exception e) {
			log.info("content cannot be transformed to JSONObject");
			return null;
		}
		if(JSObj==null){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONObject photosObj = (JSONObject)JSObj.get("photos");
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("stat", stat);
		prMetadata.put("message", message);
		if(photosObj!=null){
			String page = StringUtils.valueOf(photosObj.get("page"));
			String pages = StringUtils.valueOf(photosObj.get("pages"));
			prMetadata.put("page", page);
			prMetadata.put("pages", pages);
			JSONArray photoArr = (JSONArray)photosObj.get("photo");
			if(photoArr!=null){
				Iterator<JSONObject> photoIterator= photoArr.iterator();
				while(photoIterator.hasNext()){
					JSONObject contObj = photoIterator.next();
					Object id = contObj.get("id");
					if(id==null || StringUtils.isEmpty(id.toString())){
						//log.warn("user name is empty:"+infoObj.toString());
						continue;
					}
					imagesList.add(contObj);
				}
			}
		}
		prMetadata.put(Constants.FLICKR_IMAGELIST, imagesList);
		result.setMetadata(prMetadata);
		return result;
	}

	private ParseResult parserUserList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray usersList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
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
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		JSONObject userinfo = (JSONObject)JSObj.get("person");
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
		return result;
	}
	
	public FlickrUserDetailParser(Configuration conf,RunState state){
		setConf(conf);
	}
	
}
