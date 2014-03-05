package com.sdata.proxy.resource;

import java.util.Map;

import com.sdata.core.resource.Resource;

import weibo4j.Weibo;

/**
 * Weibo resource with email and password
 * 
 * @author zhufb
 *
 */
public class WeiboResource extends Resource {

	protected static final String Resource_email = "email";
	protected static final String Resource_password = "password";
	private String email;
	private String password;
	private String cookie ;
	
	public WeiboResource(Map<String, Object> map) {
		super(map);
		this.email = super.get(Resource_email);
		this.password = super.get(Resource_password);
		this.cookie = Weibo.getCookie(email,password);
	}

	public String getEmail() {
		return email;
	}


	public String getPassword() {
		return password;
	}
	
	public String getCookie(){
		return cookie;
	}
}
