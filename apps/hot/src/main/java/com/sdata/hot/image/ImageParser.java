package com.sdata.hot.image;

import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.hot.Hot;
import com.sdata.hot.fetcher.image.instagram.InstagramApi;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class ImageParser extends SdataParser{
	
	private int count;
	private InstagramApi api;
	private String[] SG_TAGS = new String[] { "singapore", "新加坡", "Aljunied",
			"Ang Mo Kio", "Anson Road", "Ayer Rajah", "Balestier Road",
			"Bartley", "Bedok", "Bedok Reservoir", "Bedok Town Park", "Bishan",
			"Boon Keng", "Boon Lay", "Bugis", "Bukit Chandu", "Bukit Gombak",
			"Bukit Merah", "Bukit Panjang", "Bukit Prumei", "Bukit Timah",
			"Buona Vista", "Chai Chee", "Choa Chu Kang", "Clementi",
			"Delta Estate", "Dixil", "Dover", "Dragon View Park",
			"Dunearn Estate", "East View Garden", "Eastern Gardens",
			"Eden Park", "Eng Khong Gardens", "Ewart Park", "Faber Garden",
			"Faber Hills", "Fernhill Gardens", "Fish Farming Estate",
			"Frankel Estate", "Fuyong Estate", "Gay World Park", "Geylang",
			"Geylang Serai Village", "Ghim Moh", "Goldhill Gardens",
			"Hai Sing Park", "Happy Gardens", "Havelock Estate",
			"Havelock/Delta Estate", "Heap Guan", "Heap Guan Village",
			"Hillcrest Park", "Holland", "Holland Grove Park",
			"Holland Village", "Hong Kah", "Hong Kah Village",
			"Hong Kong Park", "Hong Leong Garden", "Hougang", "Huat Choe",
			"Huat Choe Village", "Hume Heights", "Hun Yeang",
			"Hun Yeang Village", "Island View Estate", "Jalan Kayu", "Jurong",
			"Jurong East", "Jurong East New Town", "Jurong Village",
			"Jurong West", "Jurong West New Town", "Kallang", "Kallang",
			"Whampoa", "Kallang Bahru", "Lavender", "Lower Delta", "Mandai",
			"Mandai Road Village", "Marine Parade", "Marsiling Estate",
			"Maryland Estate", "Mata Ikan", "Mata Ikan Village",
			"Matilda Estate", "Mayflower Estate", "McMahon Park",
			"Medway Park", "Mok Peng Hiang Estate", "Mount Pleasant",
			"Mount Rosie", "Mount Vernon", "Namazie Estate", "Nanyang",
			"Nee Soon", "Nee Soon Estate", "Nee Soon Village", "Nepal Park",
			"Ng Kay Boon Estate", "Novena", "Oei Tiong Ham Park",
			"Old Kallang Airport Estate", "Old Upper Thomson", "Ong Lee",
			"Ong Lee Village", "Opera Estate", "Padang Terbakar Village",
			"Pandan Garden", "Pandan Valley", "Pasir Panjang",
			"Pasir Panjang Village", "Pasir Ris", "Pasir Ris New Town",
			"Pasir Ris Village", "Paya Lebar", "People's Park",
			"Peoples Garden", "Perseverance Estate", "Phoenix Park", "Pioneer",
			"Playfair Estate", "Potong Pasir", "Prince Edward Park",
			"Princess Elizabeth Estate", "Pulau Minyak", "Pulau Ubin",
			"Pulau Ubin Village", "Punggol", "Punggol Estate",
			"Punggol New Town", "Punggol Village", "Queen Astrid Park",
			"Queenstown", "Queenstown Estate", "Quemoy Park",
			"Race Course Village", "Radin Mas", "Raffles Park", "Raya Garden",
			"Rebecca Park", "Rochester Park", "Rose Garden", "Saga Village",
			"Saint Michael's Estate", "Sarang Rimau", "Sea View Estate",
			"Seletar", "Seletar Hills Estate", "Sembawang",
			"Sembawang Hills Estate", "Sembawang New Town",
			"Sembawang Village", "Seminoi", "Sengkang New Town",
			"Sennett Estate", "Serangoon", "Serangoon Garden Estate",
			"Serangoon New Town", "Serangoon Village", "Shamrock Park",
			"Siew Lim Park", "Simei", "Simei Estate", "Simpang Bedok",
			"Simpang Bedok Village", "Sin Watt Estate", "Sit Estate",
			"Somapah", "Somapah Changi", "Somapah Changi Village",
			"Somapah Serangoon", "Sommerville Estate", "Song Hah Estate",
			"Soo Chow Garden", "Spring Park Estate", "Springleaf Park",
			"Sungai Mandai", "Sungai Mandai Village", "Sungai Simpang",
			"Sungai Unum Estate", "Sungei Simpang Village", "Sussex Estate",
			"Swiss Cottage Estate", "Tai Hwan Gardens", "Tai Kheng Gardens",
			"Tai Seng", "Taman Jurong", "Tampines", "Tampines Estate",
			"Tampines New Town", "Tan Hua Gek Estate", "Tanglin",
			"Tanglin Halt", "Tanglin Hill", "Tanjong Pagar",
			"Tay Keng Loon Estate", "Teacher's Housing Estate",
			"Teban Gardens", "Teck Chong Estate", "Teck Hock",
			"Teck Hock Village", "Telok Blangah", "Telok Blangah Estate",
			"Telok Blangah New Town", "Tengah", "Tengah Village", "Thomson",
			"Thomson Garden Estate", "Thomson Ridge Estate",
			"Thomson Rise Estate", "Thomson Village", "Thong Hoe",
			"Thong Hoe Village", "Tian Guan Estate", "Tiong Baharu",
			"Tiong Bahru Estate", "Tiong Guan Estate", "Toa Payoh",
			"Toa Payoh New Town", "Tua Kang Lye", "Tuas", "Tuas Village",
			"Tumasik", "Tyersall Park", "Ulu Bedok", "Ulu Bedok Village",
			"Victoria Park", "Watten Estate", "Watten Park", "Wessex Estate",
			"West Coast Village", "Windsor Park Estate", "Woodlands",
			"Woodleigh Park", "Woollerton Park", "Xilin Estate", "Yan Kit",
			"Yan Kit Village", "Yew Tee", "Yew Tee Village", "Yio Chu Kang",
			"Yio Chu Kang Estate", "Yio Chu Kang Village", "Yishun",
			"Yishun New Town", "York Hill Estate", "Yow Toe", "Changi",
			"Orchard", "Dhoby Ghaut", "Bukit Panjang New Town", "Eunos",
			"Sungei Kadut", "Marina Centre", "Sengkang", "Tiong Bahru",
			"Kampong Pasir Ris", "Choa Chu Kang New Town", "Chong Pang",
			"Woodlands New Town", "Siglap", "Kranji", "Tampines East",
			"Outram", "Kampong Loyang", "Rochor", "Newton",
			"Asian Civilisations Museum", "BCA Gallery",
			"Butterfly Park   Insect Kingdom", "Cheng Ho Cruise",
			"Chinatown Heritage Centre", "Chinese Heritage Centre",
			"Civil Defence Heritage Gallery", "Escape Theme Park",
			"Fort Siloso", "Fu Tak Chi Museum", "Grassroots Heritage Centre",
			"Healthzone", "HiPPOtours", "Hua Song Museum", "IRAS Gallery",
			"Jurong Bird Park", "Kong Hiap Memorial Museum",
			"Labrador Secret Tunnel", "Land Transport Gallery",
			"LilliPutt Indoor Mini Golf", "Malay Heritage Centre",
			"Malay Village", "Memories At Old Ford Factory",
			"Mint Museum of Toys", "National Orchid Garden", "Night Safari",
			"NEWater Visitor Centre", "Ngee Ann Cultural Centre", "NUS Museum",
			"Peranakan Museum", "Police Heritage Centre",
			"Raffles Museum of Biodiversity Research", "Red Dot Design Museum",
			"Reflections at Bukit Chandu",
			"Republic of Singapore Air Force Museum", "Sentosa", "Snow City",
			"Sun Yat Sen Nanyang Memorial Hall",
			"Tan Tock Seng Hospital's Heritage Museum", "The Changi Museum",
			"The Battle Box", "The Merlion", "The SGH Museum",
			"Tiger Sky Tower", "VivoCity" };
	
	public ImageParser(Configuration conf){
		this.count = conf.getInt("crawl.count", 3);
		this.api = new InstagramApi(conf);
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		while(result.getListSize()<count){
			JSONObject JSObj = api.getPopular();
			this.sleep(1);
			if(JSObj.isNullObject()){
				continue;
			}
			JSONObject meta = JSObj.getJSONObject("meta");
			if(meta.isNullObject()||!"200".equals(meta.getString("code"))){
				continue;
			}
			JSONArray mediaData = (JSONArray)JSObj.get("data");
			if(mediaData==null||mediaData.size() ==0){
				continue;
			}

			for(int i=1;i<=mediaData.size();i++){
				FetchDatum datum = new FetchDatum();
				JSONObject json = mediaData.getJSONObject(i-1);
				if(!isSingapore(json)){
					continue;
				}
				if(isHave(result,json)){
					continue;
				}
//				byte[] rk = HotUtils.getRowkey(Hot.Image.getValue(), fetTime, i);
				Object pubTime = DateFormat.strToDate(json.get("created_time"));
				datum.addMetadata("pub_time", pubTime);
				JSONObject caption = json.getJSONObject("caption");
				if(!caption.isNullObject()&&!StringUtils.isEmpty(caption.getString("text"))){
					datum.addMetadata("content", caption.get("text"));
				}
				JSONObject images = json.getJSONObject("images");
				Object image = images.getJSONObject("standard_resolution").get("url");
				JSONObject user = json.getJSONObject("user");
				if(!user.isNullObject()){
					datum.addMetadata("uname", user.get("full_name"));
					datum.addMetadata("head", user.get("profile_picture"));
				}
				JSONObject likes = json.getJSONObject("likes");
				if(!likes.isNullObject()){
					datum.addMetadata("likes", likes.get("count"));
				}
				JSONObject comments = json.getJSONObject("comments");
				if(!comments.isNullObject()){
					datum.addMetadata("comments", comments.get("count"));
				}
				JSONObject location = json.getJSONObject("location");
				if(!location.isNullObject()){
					datum.addMetadata("geo", location.get("latitude")+","+location.get("longitude"));
				}
				
//				datum.addMetadata("rk", rk);
				datum.addMetadata("id", json.get("id"));
				datum.addMetadata("type", Hot.Image.getValue());
//				datum.addMetadata("fet_time", fetTime);
				datum.addMetadata("image", image);
				datum.addMetadata("target", image);
				datum.addMetadata("link", json.get("link"));
				datum.setId(json.get("id"));
				datum.setUrl(image.toString());
				result.addFetchDatum(datum);
				if(result.getListSize()>=count){
					break;
				}
			}
		}
		
		Date fetTime = new Date();
		for(int i=1;i<=count;i++){
			FetchDatum datum = result.getFetchList().get(i-1);
			byte[] rk = HotUtils.getRowkey(Hot.Image.getValue(), fetTime, i);
			datum.addMetadata("rk", rk);
			datum.addMetadata("rank", i);
			datum.addMetadata("fet_time", fetTime);
		}
		return result;
	}

	private boolean isHave(ParseResult result,JSONObject json){
		Object id = json.get("id");
		if(id == null){
			return true;
		}
		for(FetchDatum datum:result.getFetchList()){
			if(id.equals(datum.getId())){
				return true;
			}
		}
		return false;
	}
	
	protected void sleep(int s) {
		try {
			Thread.sleep(s * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private boolean isSingapore(JSONObject json){
		Object tags = MapUtils.getInter(json, "tags");
		if(tags!=null&&tags instanceof List){
			List list = (List)tags;
			if(list.contains("singapore")){
				return true;	
			}
			if(list.contains("sg")){
				return true;	
			}
			if(list.contains("singaporean")){
				return true;	
			}
		}
		
		Object text = MapUtils.getInter(json, "caption/text");
		if(text!=null){
			String str = text.toString();
			for(String s:SG_TAGS){
				Pattern compile = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
				Matcher matcher = compile.matcher(str);
				if(matcher.find()){
					return true;
				}
			}
		}

		//singapore 1.2-1.5,103.5-104.2 
		Object lat = MapUtils.getInter(json, "location/latitude");
		Object lng = MapUtils.getInter(json, "location/longitude");
		if(lat!=null&&lng!=null){
			Double dlat = Double.valueOf(lat.toString());
			Double dlng = Double.valueOf(lng.toString());
			if(dlat>=1.2&&dlat<=1.5&&dlng>=103.5&&dlng<=104.2){
				return true;
			}
		}
		return false;
	}
}
