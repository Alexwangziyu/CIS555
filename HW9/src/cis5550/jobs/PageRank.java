package cis5550.jobs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {
	 public static void run(FlameContext context, String[] args) throws Exception {
//		 FlameContextImpl context = new FlameContextImpl(null);
		 System.out.println("GOOGO");
	     FlameRDD rawData = context.fromTable("pt-crawl", row ->{
	    	 String url = row.get("url");
	    	 String p = row.get("page");
	    	 
	    	 List<String> results = extracturl(p);
	           
	           //normalize url
	           String[] seedurl = URLParser.parseURL(url);
               int lastIndex = seedurl[3].lastIndexOf("/");
               String path = "";
               if (lastIndex != -1) {
                   path = seedurl[3].substring(0, lastIndex + 1);
               }
               String beforepath = "";
               if (seedurl[2] != null) {
                   beforepath = seedurl[0] + "://" + seedurl[1] + ":" + seedurl[2];
               } else {
                   if (seedurl[0].toLowerCase().equals("http")) {
                       beforepath = seedurl[0] + "://" + seedurl[1] + ":" + "80";
                   } else if (seedurl[0].toLowerCase().equals("https")) {
                       beforepath = seedurl[0] + "://" + seedurl[1] + ":" + "8000";
                   }
               }
//               System.out.println("22222");
               //get internal url list
               String internalurls = "1.0,1.0";
               HashSet<String> uniqueurl = new HashSet<>();
	           for (String rawurl : results) {
	               // System.out.println(rawurl);
	               int fragmentIndex = rawurl.indexOf("#");
	               if (fragmentIndex != -1) {
	                   rawurl = rawurl.substring(0, fragmentIndex);
	               }
	               if (rawurl.equals("")) {
	            	   if ( uniqueurl.add(url)) {
		            	   internalurls+=(","+url);
		               }
	            	   continue;
	               }
	               String[] splittedurl = URLParser.parseURL(rawurl);
	               if(splittedurl[0] != null) {
	            	   if (splittedurl[2] != null) {
	                   } else {
	                       if (splittedurl[0].toLowerCase().equals("http")) {
	                    	   rawurl = splittedurl[0] + "://" + splittedurl[1] + ":" + "80"+splittedurl[3];
	                       } else if (seedurl[0].toLowerCase().equals("https")) {
	                    	   rawurl = splittedurl[0] + "://" + splittedurl[1] + ":" + "8000"+splittedurl[3];
	                       }
	                   }
	            	   if ( uniqueurl.add(rawurl)) {
		            	   internalurls+=(","+rawurl);
		               }
	            	   continue;
	               }
	               if (rawurl.startsWith("/")) {
	                   rawurl = beforepath + rawurl;
	               } else {
	                   rawurl = beforepath + path + rawurl;
	               }
	               while (rawurl.contains("..")) {
	                   int index = rawurl.indexOf("..");
	                   int slashIndex = rawurl.lastIndexOf('/', index - 2);
	                   if (slashIndex != -1) {
	                       rawurl = rawurl.substring(0, slashIndex) + rawurl.substring(index + 2);
	                   } else {
	                       break;
	                   }
	               }
	               if ( uniqueurl.add(rawurl)) {
	            	   internalurls+=(","+rawurl);
	               }
	           }
	           return url+"+"+internalurls;
	           
	     });
	     
	     FlamePairRDD result = rawData.mapToPair(str -> {
	    	 String[] list = str.split("\\+", 2);
	    	 return new FlamePair(list[0],list[1]);
	     });
	     //result= （url，1.0,1.0,xxxx.com,....)
	     System.out.println("33333");
	     int iii =0;
	     while(true) {
		    System.out.println(iii+"iii");
		    iii+=1;
		    FlamePairRDD result1 = result.flatMapToPair(pair -> {
		    	 String[] urls = pair._2().split(",");
	    	 double rc = Double.parseDouble(urls[0]);
	    	 double rp = Double.parseDouble(urls[1]);
	    	 List<FlamePair> pairlist = new ArrayList<>();
	    	 int n = urls.length -2;
	    	 for(int i =2; i< urls.length;i++){
	    		 System.out.println(urls[i]+0.85*rc/n);
	    		 FlamePair p = new FlamePair(urls[i],(0.85*rc/n)+"");
	    		 pairlist.add(p);
	    	 }
		    	 FlamePair p = new FlamePair(pair._1(),"0");
	    		 pairlist.add(p);
		    	 return pairlist; 
		     });
		     FlamePairRDD finalresult = result1.foldByKey("0", (v1,v2)->{
		    	 Double v11 = Double.parseDouble(v1);
		    	 Double vresult =  Double.parseDouble(v1)+Double.parseDouble(v2);
		    	 return vresult+"";
		     });
		     FlamePairRDD joinedresult = result.join(finalresult);
		     result = joinedresult.flatMapToPair(pair ->{
		    	 String[] urls = pair._2().split(",");
		    	 System.out.println(pair._1()+"RCRCRC:"+urls[urls.length-1]);
		    	 System.out.println("rp:"+urls[0]);
		    	 String rc = (Double.parseDouble(urls[urls.length-1])+0.15)+"";
		    	 String rp = urls[0];
		    	 urls[0]=rc;
		    	 urls[1]=rp;
		    	 List<FlamePair> pairlist = new ArrayList<>();
	//	    	 int n = urls.length -2;
		    	 String pairb = "";
		    	 for(int i =0; i< urls.length-1;i++){
		    		 if(i ==0) {
		    			 pairb += urls[i];
		    		 }
		    		 else {
		    			 pairb += ","+urls[i];
		    		 }
		    	 }
		    	 System.out.println(pair._1()+"+++"+pairb);
		    	 pairlist.add(new FlamePair(pair._1(),pairb));
		    	 return pairlist;     	 
		     });
		     FlameRDD diff = result.flatMap(pair -> {
		    	 List<String> diff1 = new ArrayList<>();
		    	 String[] urls = pair._2().split(",");
		    	 String difference = "" + Math.abs((Double.parseDouble(urls[0])-Double.parseDouble(urls[1])));
		    	 diff1.add(difference);
		    	 return diff1;
		     });
		     String max = diff.fold("0",(s1,s2)->{
		     	if (Double.parseDouble(s2)>Double.parseDouble(s1)) {
		     		return s2;
		     	}else {
		     		return s1;
		     	}
		     });
			     if(Double.parseDouble(max)<=Double.parseDouble(args[0])) {
			    	 break;
			     }
	     }
	     FlamePairRDD finalpr = result.flatMapToPair(pair -> {
	    	 List<FlamePair> prlist = new ArrayList<>();
	    	 String[] urls = pair._2().split(",");
	    	 Row r = new Row(Hasher.hash(pair._1()));
	    	 r.put("rank", urls[0]);
	    	 context.getKVS().putRow("pt-pageranks", r);
	    	 return prlist;
	     });
	 }
	    public static List<String> extracturl(String URL) {
	        List<String> extractedUrls = new ArrayList<>();
	        String[] tags = URL.split("<");

	        for (String tag : tags) {
	            // Remove any leading and trailing white spaces
	            tag = tag.trim();

	            // Check if it's not an empty string and starts with "a" (anchor tag)
	            if (!tag.isEmpty() && tag.startsWith("a")) {
	                // Find the "href" attribute
	                int hrefIndex = tag.indexOf("href=\"");

	                if (hrefIndex != -1) {
	                    int urlStart = hrefIndex + 6; // Start of the URL
	                    int urlEnd = tag.indexOf("\"", urlStart); // End of the URL
	                    if (urlEnd != -1) {
	                        String extractedUrl = tag.substring(urlStart, urlEnd);
	                        extractedUrls.add(extractedUrl);
	                    }
	                }
	            }
	        }

	        return extractedUrls;
	    }
}
