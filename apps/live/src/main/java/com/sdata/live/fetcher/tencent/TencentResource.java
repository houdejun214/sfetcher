package com.sdata.live.fetcher.tencent;

import java.util.Map;

import com.sdata.live.resource.Resource;
import com.tencent.weibo.utils.Tencent;

/**
 * Tencent resource with qq and password
 * 
 * @author zhufb
 *
 */
public class TencentResource extends Resource {

	protected static final String Resource_qq = "qq";
	protected static final String Resource_password = "password";
	private String qq;
	private String password;
	private String cookie ;
	
	public TencentResource(Map<String, Object> map) {
		super(map);
		this.qq = super.get(Resource_qq);
		this.password = super.get(Resource_password);
		this.cookie = Tencent.getCookie(qq,password);
	}

	public String getQq() {
		return qq;
	}

	public String getPassword() {
		return password;
	}
	
	public String getCookie(){
		return cookie;
	}
}
