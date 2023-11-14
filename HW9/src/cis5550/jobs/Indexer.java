package cis5550.jobs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cis5550.flame.*;

public class Indexer {
	 public static void run(FlameContext context, String[] args) throws Exception {
	        // Load the data from the pt-crawl table
//		 FlameContextImpl context = new FlameContextImpl(null);
		 context.output("zz");
	     FlameRDD rawData = context.fromTable("pt-crawl", row ->{
	    	 String url = row.get("url");
	    	 String p = row.get("page");
	    	 if (p !=null) {
	    		 return url+"+"+p ;
	    	 }
	    	 else {
	    		 return null;
	    	 }
	     });
	     FlamePairRDD result = rawData.mapToPair(str -> {
	    	 String[] list = str.split("\\+", 2);
	    	 return new FlamePair(list[0],list[1]);
	    	 
	     });
	    @SuppressWarnings("unchecked")
	    FlamePairRDD mappair =  result.flatMapToPair(pair -> {
			List<FlamePair> pairlist = new ArrayList<>();
			String page = pair._2().replaceAll("\\<.*?\\>", " ") // Remove HTML tags
					.replaceAll("[^\\w\\d\\s]+", " ") // Remove all punctuation
					.toLowerCase();
			HashSet<String> uniqueWords = new HashSet<>();
	        for (String word : page.split("\\s+")) {
	            if (!word.isEmpty() && uniqueWords.add(word)) {
	            	pairlist.add(new FlamePair(word, pair._1()));
	            }
	        }
	        return pairlist;
	     });


//	    FlamePairRDD mappair =  result.flatMapToPair(pair -> {
//	   			List<FlamePair> pairlist = new ArrayList<>();
//	   			String page = pair._2().replaceAll("<[^>]+>", " ");
//	   			page = page.replaceAll("[^\\w\\d\\s]+", " ");
//	   	        // 移除多余的空格，并转换为小写
//	   			page = page.trim().toLowerCase();
//	   			System.out.println("PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP:"+page);
//	   			HashSet<String> uniqueWords = new HashSet<>();
//	   	        for (String word : page.split(" ")) {
////	   	        	System.out.println(word);
//	   	            if (!word.isEmpty() && uniqueWords.add(word)) {
//	   	            	pairlist.add(new FlamePair(word, pair._1()));
//	   	            }
//	   	        }
//	   	        return pairlist;
//	   	     });
	    FlamePairRDD finalresult = mappair.foldByKey("",((url1,url2) -> {
	    	if(url1.equals("")){
	    	return url2;
	    }
	    	return url1+","+url2;
	    }));
	    finalresult.saveAsTable("pt-index");
	     return;
	    }
	 public void main() throws Exception {
		 System.out.println("go!");
	 }

}
