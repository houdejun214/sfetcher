package com;  
  
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Date;

import net.sf.json.JSONObject;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.ProxyHost;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;

import com.lakeside.download.http.HttpPageLoader;

public class TestPost {     
    
	public static final String email = "lmsnextsearch@gmail.com";
	public static final String passwd = "lmsnextsearch";
	
    public static void testPost() throws IOException {     
    	try {
    		PostMethod post = new PostMethod("http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.3.17)");
    		String baseEmail = encodeAccount(email);
    		String serverTime = getServerTime();
    		String nonce = makeNonce(6);
    	    String sp = new SinaSSOEncoder().encode(passwd, serverTime, nonce);
    		post.addParameter("entry", "weibo");
    		post.addParameter("gateway", "1");
    		post.addParameter("from", "");
    		post.addParameter("savestate", "7");
    		post.addParameter("useticket", "1");
    		post.addParameter("ssosimplelogin", "1");
    		post.addParameter("vsnf", "1");
    		post.addParameter("vsnval", "");
    		post.addParameter("service", "miniblog");
    		post.addParameter("servertime",serverTime);
    		post.addParameter("nonce", nonce);
    		post.addParameter("pwencode", "wsse");
    		post.addParameter("su", baseEmail);
    		post.addParameter("sp",sp);
    		post.addParameter("encoding", "utf-8");
    		post.addParameter("url", "http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack");
    		post.addParameter("returntype", "META");
    		
    		String ip = "211.94.93.224";
			Integer port = 3128;
			ProxyHost proxyHost = new ProxyHost(ip,port);
    		HttpClient client = new HttpClient();
    		HostConfiguration hostConfiguration = client.getHostConfiguration();
    		hostConfiguration.setProxyHost(proxyHost);
    		client.executeMethod(post);
    		Header[] resHeader = post.getResponseHeaders("Set-Cookie");
			String cookiestr = "";
			for (Header header : resHeader) {
				String cookie = header.toString().replace("Set-Cookie:", "").trim();
				cookiestr += cookie.substring(0,cookie.indexOf(";")) + ";";
			}
			cookiestr = cookiestr.concat("wvr=4;").concat("un=").concat(email).concat(";");
			System.out.println(cookiestr);
			System.out.println((post.getResponseBodyAsString() + "\n"));
			
			GetMethod get = new GetMethod("http://open.weibo.com/tools/aj_apitest.php");
			Header header = new Header("Cookie",cookiestr);
			get.setQueryString("appkey=3557240077");
 			get.setRequestHeader(header);
 			client.executeMethod(get);
 			String result = get.getResponseBodyAsString();
 			if(!result.startsWith("{")){
 				System.out.println("error");
 				return;
 			}
 			JSONObject json = JSONObject.fromObject(result);
 			System.out.println(json.get("token"));
 			
			//System.out.println(post.getResponseBodyAsString());
	}	catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }     
    
    public static void main(String[] args) throws IOException {  
    	HttpPageLoader.getDefaultPageLoader();
    
    }     
    
    //六位随机数nonce的产生
    private static String makeNonce(int len){
            String x="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            String str = "";
            for(int i=0;i<len;i++){
                str+=x.charAt((int) (Math.ceil(Math.random()*1000000)%x.length()));
            }
            return str;
        }
    //servertime的产生
    private static String getServerTime(){
            long servertime = new Date().getTime()/1000;
            return String.valueOf( servertime);
      }
    

    private static String encodeAccount(String account){
        return Base64.encodeBase64String(URLEncoder.encode(account).getBytes());
    }
    //新浪微博密码加密的算法 
   public static class SinaSSOEncoder {
        private boolean i=false;
        private int g=8;
         
        public SinaSSOEncoder(){
             
        }
        public String encode(String psw,String servertime,String nonce){
            String password;
            password=hex_sha1(""+hex_sha1(hex_sha1(psw))+servertime+nonce);
            return password;
        }
         
        private String hex_sha1(String j) {
            return h(b(f(j,j.length()*g), j.length() * g));
        }
        private String h(int[] l){
            String k = i ? "0123456789ABCDEF" : "0123456789abcdef";
            String m = "";
            for (int j = 0; j < l.length * 4; j++) {
                m += k.charAt((l[j >> 2] >> ((3 - j % 4) * 8 + 4)) & 15) + "" + k.charAt((l[j >> 2] >> ((3 - j % 4) * 8)) & 15);
            }
            return m;
        }
         
        private int[] b(int[] A,int r){
            A[r>>5]|=128<<(24-r%32);
            A[((r+64>>9)<<4)+15]=r;
            int[] B = new int[80];
            int z = 1732584193;
            int y = -271733879;
            int v = -1732584194;
            int u = 271733878;
            int s = -1009589776;
            for (int o = 0; o < A.length; o += 16) {
                int q = z;
                int p = y;
                int n = v;
                int m = u;
                int k = s;
                for (int l = 0; l < 80; l++) {
                    if (l < 16) {
                        B[l] = A[o + l];
                    } else {
                        B[l] = d(B[l - 3] ^ B[l - 8] ^ B[l - 14] ^ B[l - 16], 1);
                    }
                    int C = e(e(d(z, 5), a(l, y, v, u)), e(e(s, B[l]), c(l)));
                    s = u;
                    u = v;
                    v = d(y, 30);
                    y = z;
                    z = C;
                }
                z = e(z, q);
                y = e(y, p);
                v = e(v, n);
                u = e(u, m);
                s = e(s, k);
            }
            return new int[]{z,y,v,u,s};
        }
         
        private int a(int k,int j,int m,int l){
            if(k<20){return(j&m)|((~j)&l);};
            if(k<40){return j^m^l;};
            if(k<60){return(j&m)|(j&l)|(m&l);};
            return j^m^l;
        }
         private int c(int j){
            return(j<20)?1518500249:(j<40)?1859775393:(j<60)?-1894007588:-899497514;
        }
        private int e(int j, int m) {
            int l = (j & 65535) + (m & 65535);
            int k = (j >> 16) + (m >> 16) + (l >> 16);
            return (k << 16) | (l & 65535);
        }
        private int d(int j,int k){
            return(j<<k)|(j>>>(32-k));
        }
         
        private int[] f(String m,int r){
            int[] l;
            int j = (1<<this.g)-1;
            int len=((r+64>>9)<<4)+15;
            int k;
            for(k=0;k<m.length()*g;k+=g){
                len = k>>5>len?k>>5:len;
            }
            l = new int[len+1];
            for(k=0;k<l.length;k++){
                l[k]=0;
            }
            for(k=0;k<m.length()*g;k+=g){
                l[k>>5]|=(m.charAt(k/g)&j)<<(24-k%32);
            }
            return l;
        }
    }
    
}   