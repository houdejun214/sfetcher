package com.sdata.component.fetcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;


public class TencentLogin{
		 
		public String md5(String p){
			return hex_md5(p);
		}
	 
		public String md5_3(String p){
	 
			int s[] = null;
		    s = core_md5(str2binl(p), p.length() * 8);
		    s = core_md5(s, 16 * 8);
		    s = core_md5(s, 16 * 8);
		    return binl2hex(s);
		}
	 
		public String hex_md5(String p) {
		    return binl2hex(core_md5(str2binl(p), p.length() * 8));
		}
	 
		public int[] core_md5(int[] s, int k){
			s[k >> 5] |= 128 << (k % 32);
			s[(((k + 64) >>> 9) << 4) + 14] = k;
			int o = 1732584193;
			int n = -271733879;
			int m = -1732584194;
			int l = 271733878;
			for (int g = 0; g < s.length; g += 16) {
		        int j = o;
		        int h = n;
		        int f = m;
		        int e = l;
		        o = md5_ff(o, n, m, l, s[g + 0], 7, -680876936);
		        l = md5_ff(l, o, n, m, s[g + 1], 12, -389564586);
		        m = md5_ff(m, l, o, n, s[g + 2], 17, 606105819);
		        n = md5_ff(n, m, l, o, s[g + 3], 22, -1044525330);
		        o = md5_ff(o, n, m, l, s[g + 4], 7, -176418897);
		        l = md5_ff(l, o, n, m, s[g + 5], 12, 1200080426);
		        m = md5_ff(m, l, o, n, s[g + 6], 17, -1473231341);
		        n = md5_ff(n, m, l, o, s[g + 7], 22, -45705983);
		        o = md5_ff(o, n, m, l, s[g + 8], 7, 1770035416);
		        l = md5_ff(l, o, n, m, s[g + 9], 12, -1958414417);
		        m = md5_ff(m, l, o, n, s[g + 10], 17, -42063);
		        n = md5_ff(n, m, l, o, s[g + 11], 22, -1990404162);
		        o = md5_ff(o, n, m, l, s[g + 12], 7, 1804603682);
		        l = md5_ff(l, o, n, m, s[g + 13], 12, -40341101);
		        m = md5_ff(m, l, o, n, s[g + 14], 17, -1502002290);
		        n = md5_ff(n, m, l, o, s[g + 15], 22, 1236535329);
		        o = md5_gg(o, n, m, l, s[g + 1], 5, -165796510);
		        l = md5_gg(l, o, n, m, s[g + 6], 9, -1069501632);
		        m = md5_gg(m, l, o, n, s[g + 11], 14, 643717713);
		        n = md5_gg(n, m, l, o, s[g + 0], 20, -373897302);
		        o = md5_gg(o, n, m, l, s[g + 5], 5, -701558691);
		        l = md5_gg(l, o, n, m, s[g + 10], 9, 38016083);
		        m = md5_gg(m, l, o, n, s[g + 15], 14, -660478335);
		        n = md5_gg(n, m, l, o, s[g + 4], 20, -405537848);
		        o = md5_gg(o, n, m, l, s[g + 9], 5, 568446438);
		        l = md5_gg(l, o, n, m, s[g + 14], 9, -1019803690);
		        m = md5_gg(m, l, o, n, s[g + 3], 14, -187363961);
		        n = md5_gg(n, m, l, o, s[g + 8], 20, 1163531501);
		        o = md5_gg(o, n, m, l, s[g + 13], 5, -1444681467);
		        l = md5_gg(l, o, n, m, s[g + 2], 9, -51403784);
		        m = md5_gg(m, l, o, n, s[g + 7], 14, 1735328473);
		        n = md5_gg(n, m, l, o, s[g + 12], 20, -1926607734);
		        o = md5_hh(o, n, m, l, s[g + 5], 4, -378558);
		        l = md5_hh(l, o, n, m, s[g + 8], 11, -2022574463);
		        m = md5_hh(m, l, o, n, s[g + 11], 16, 1839030562);
		        n = md5_hh(n, m, l, o, s[g + 14], 23, -35309556);
		        o = md5_hh(o, n, m, l, s[g + 1], 4, -1530992060);
		        l = md5_hh(l, o, n, m, s[g + 4], 11, 1272893353);
		        m = md5_hh(m, l, o, n, s[g + 7], 16, -155497632);
		        n = md5_hh(n, m, l, o, s[g + 10], 23, -1094730640);
		        o = md5_hh(o, n, m, l, s[g + 13], 4, 681279174);
		        l = md5_hh(l, o, n, m, s[g + 0], 11, -358537222);
		        m = md5_hh(m, l, o, n, s[g + 3], 16, -722521979);
		        n = md5_hh(n, m, l, o, s[g + 6], 23, 76029189);
		        o = md5_hh(o, n, m, l, s[g + 9], 4, -640364487);
		        l = md5_hh(l, o, n, m, s[g + 12], 11, -421815835);
		        m = md5_hh(m, l, o, n, s[g + 15], 16, 530742520);
		        n = md5_hh(n, m, l, o, s[g + 2], 23, -995338651);
		        o = md5_ii(o, n, m, l, s[g + 0], 6, -198630844);
		        l = md5_ii(l, o, n, m, s[g + 7], 10, 1126891415);
		        m = md5_ii(m, l, o, n, s[g + 14], 15, -1416354905);
		        n = md5_ii(n, m, l, o, s[g + 5], 21, -57434055);
		        o = md5_ii(o, n, m, l, s[g + 12], 6, 1700485571);
		        l = md5_ii(l, o, n, m, s[g + 3], 10, -1894986606);
		        m = md5_ii(m, l, o, n, s[g + 10], 15, -1051523);
		        n = md5_ii(n, m, l, o, s[g + 1], 21, -2054922799);
		        o = md5_ii(o, n, m, l, s[g + 8], 6, 1873313359);
		        l = md5_ii(l, o, n, m, s[g + 15], 10, -30611744);
		        m = md5_ii(m, l, o, n, s[g + 6], 15, -1560198380);
		        n = md5_ii(n, m, l, o, s[g + 13], 21, 1309151649);
		        o = md5_ii(o, n, m, l, s[g + 4], 6, -145523070);
		        l = md5_ii(l, o, n, m, s[g + 11], 10, -1120210379);
		        m = md5_ii(m, l, o, n, s[g + 2], 15, 718787259);
		        n = md5_ii(n, m, l, o, s[g + 9], 21, -343485551);
		        o = safe_add(o, j);
		        n = safe_add(n, h);
		        m = safe_add(m, f);
		        l = safe_add(l, e);
		    }
			int[] rs = new int[16];
			rs[0] = o;
			rs[1] = n;
			rs[2] = m;
			rs[3] = l;
			return rs;
		}
	 
		public int[] str2binl(String p){
	 
			int[] s = new int[16];
			int a = 254;
			for(int i = 0; i < p.length() * 8; i += 8){
				s[ i >> 5] |= ((p.charAt(i / 8)) & a + 1) << (i % 32);
			}
			return s;
		}
	 
		public static void main(String[] args) throws MalformedURLException, IOException{
			TencentLogin q = new TencentLogin();
			String verifyURL = "http://check.ptlogin2.qq.com/check?uin=285900725&appid=46000101&r=0.37908964480776913";
			String loginURL = "http://ptlogin2.qq.com/login?";
			String redirectURL = "";
			String cookie = "";
			String qqn = "285900725";
			String md5Pass = "";
			String passwd ="doujiang520";
			String verifyCode = "";
			HttpURLConnection urlConn = (HttpURLConnection)(new URL(verifyURL)).openConnection();
			urlConn.setDoOutput(true);
			urlConn.setDoInput(true);
			urlConn.setUseCaches(false);
			urlConn.setAllowUserInteraction(false);
			urlConn.setRequestMethod("GET");
			urlConn.connect();
			String cookie1 = urlConn.getHeaderField("Set-Cookie");
			BufferedReader br = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
			StringBuffer sb = new StringBuffer();
			String str = br.readLine();
			while(null != str){
				sb.append(str);
				str = br.readLine();
			}
			br.close();
			urlConn.disconnect();
			verifyCode = sb.substring(18, 22);
			loginURL += "u=285900725&p=";
			loginURL += q.md5(q.md5_3(passwd) + verifyCode.toUpperCase());
			loginURL += "&verifycode="+verifyCode.toUpperCase()+"&aid=46000101&u1=http%3A%2F%2Ft.qq.com&h=1&from_ui=1&fp=loginerroralert";
			urlConn = (HttpURLConnection)(new URL(loginURL)).openConnection();
			urlConn.setDoOutput(true);
			urlConn.setDoInput(true);
			urlConn.setUseCaches(false);
			urlConn.setAllowUserInteraction(false);
			urlConn.setRequestProperty("Cookie", cookie1);
			urlConn.connect();
			String[] setCookies = urlConn.getHeaderFields().get("Set-Cookie").toString().split(";");
	 
			String cookie2 = "";
			cookie2 += setCookies[30];
			cookie2 += setCookies[27];
			cookie2 += setCookies[24];
			cookie2 += setCookies[4];
			cookie2 = cookie2.replace(',', ';');
			cookie2 = cookie2.substring(2, cookie2.length());
			System.out.print(cookie2 + "<hr />");
			br = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));;
			str = br.readLine();
			sb = new StringBuffer();
			while(null != str){
				sb.append(str);
				str = br.readLine();
			}
			br.close();
			urlConn.disconnect();
		}
	 
		public int md5_cmn(int h, int e, int d, int c, int g, int f){
			return safe_add(bit_rol(safe_add(safe_add(e, h), safe_add(c, f)), g), d);
		}
	 
		public int md5_ff(int g, int f, int k, int j, int e, int i, int h) {
		    return md5_cmn((f & k) | ((~f) & j), g, f, e, i, h);
		}
		public int md5_gg(int g, int f, int k, int j, int e, int i, int h) {
		    return md5_cmn((f & j) | (k & (~j)), g, f, e, i, h);
		}
		public int md5_hh(int g, int f, int k, int j, int e, int i, int h) {
		    return md5_cmn(f ^ k ^ j, g, f, e, i, h);
		}
		public int md5_ii(int g, int f, int k, int j, int e, int i, int h) {
		    return md5_cmn(k ^ (f | (~j)), g, f, e, i, h);
		}
	 
		public int safe_add(int a, int d){
			int c = (a & 65535) + (d & 65535);
			int b = (a >> 16) + (d >> 16) + (c >> 16);
			return (b << 16) | (c & 65535);
		}
	 
		public int bit_rol(int a, int b){
			return (a << b) | (a >>> (32 - b));
		}
	 
		public String binl2hex(int[] c) {
		    String b = "0123456789ABCDEF";
		    String d = "";
		    for (int a = 0; a <c.length * 4; a++) {
		    	char x = (char)b.charAt((c[a >> 2] >> ((a % 4) * 8 + 4)) & 15);
		    	char y = (char)b.charAt((c[a >> 2] >> ((a % 4) * 8)) & 15);
		    	d += x;
		    	d += y;
		    }
		    return d.substring(0, 32);
		}
}
