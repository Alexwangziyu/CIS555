package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;
import cis5550.webserver.Route;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    post("/rdd/flatMap", (request, response) -> {
    	 String inputTable = request.queryParams("inputTable");
		 String outputTable = request.queryParams("outputTable");
		 String kvsCoordinator = request.queryParams("kvsCoordinator");
		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
		 String keyRange1 = request.queryParams("keyRangeFrom");
		 String keyRange2 = request.queryParams("keyRangeTo");
		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
		 FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
		 System.out.println(keyRange1);
		 System.out.println(keyRange2);
		 if (keyRange1.equals("null")) {
			 keyRange1=null;
		 }
		 if (keyRange2.equals("null")) {
			 keyRange2=null;
		 }
		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
		 
		 if (!Iter.hasNext()) {
			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
		 }
		    // Scan the keys in the specified key range and apply the lambda function
		    // to process the values.
		 while (Iter.hasNext()) {
	        Row row = Iter.next();
		    Iterable<String> result1 = lambda.op(row.get("value"));
		    Iterator<String> result = result1.iterator();
		    System.out.println(result1);
	        if (result != null) {
	            // Iterate over the elements in the Iterable and put them into the output table
	        	while (result.hasNext()){
	        		String item =  result.next();
	                // Create a unique row key here to ensure no overwriting
	            	String rowKey = UUID.randomUUID().toString();
	                // Store the item in the output table in the KVS
	            	kvs1.put(outputTable, rowKey, "value", item);
	            }
	        }
		    }
		    response.status(200, "OK");
		    return "OK";
    	
    });
    
    post("/rdd/p2sflatmap", (request, response) -> {
   	 String inputTable = request.queryParams("inputTable");
		 String outputTable = request.queryParams("outputTable");
		 String kvsCoordinator = request.queryParams("kvsCoordinator");
		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
		 String keyRange1 = request.queryParams("keyRangeFrom");
		 String keyRange2 = request.queryParams("keyRangeTo");
		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
		 FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
		 System.out.println(keyRange1);
		 System.out.println(keyRange2);
		 if (keyRange1.equals("null")) {
			 keyRange1=null;
		 }
		 if (keyRange2.equals("null")) {
			 keyRange2=null;
		 }
		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
		 
		 if (!Iter.hasNext()) {
			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
		 }
		    // Scan the keys in the specified key range and apply the lambda function
		    // to process the values.
		 while (Iter.hasNext()) {
	        Row row = Iter.next();
	        for (String x: row.columns()) {
	        	FlamePair fp = new FlamePair(row.key(),row.get(x));
	        	Iterable<String> result1 = lambda.op(fp);
	 		    Iterator<String> result = result1.iterator();
	 		   if (result != null) {
		            // Iterate over the elements in the Iterable and put them into the output table
		        	while (result.hasNext()){
		        		String item =  result.next();
		                // Create a unique row key here to ensure no overwriting
		            	String rowKey = UUID.randomUUID().toString();
		                // Store the item in the output table in the KVS
		            	kvs1.put(outputTable, rowKey, "value", item);
		            }
		        }
	        }
		    }
		    response.status(200, "OK");
		    return "OK";
   	
   });
    
    post("/rdd/mapToPair", (request, response) -> {
      	 String inputTable = request.queryParams("inputTable");
   		 String outputTable = request.queryParams("outputTable");
   		 String kvsCoordinator = request.queryParams("kvsCoordinator");
   		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
   		 String keyRange1 = request.queryParams("keyRangeFrom");
   		 String keyRange2 = request.queryParams("keyRangeTo");
   		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
   		 FlameRDD.StringToPair lambda = (FlameRDD.StringToPair)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
   		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
   		 System.out.println(keyRange1);
   		 System.out.println(keyRange2);
   		 if (keyRange1.equals("null")) {
   			 keyRange1=null;
   		 }
   		 if (keyRange2.equals("null")) {
   			 keyRange2=null;
   		 }
   		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
   		 
   		 if (!Iter.hasNext()) {
   			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
   		 }
   		 while (Iter.hasNext()) {
   	        Row row = Iter.next();
//   	        System.out.printf("Row row = Iter.next();");
   	        for(String i:row.columns()) {
   	        	FlamePair result = lambda.op(row.get(i));
   	        	//result = (a,pple)
//   	        	System.out.println(result.a);
//   	        	System.out.println(i);
//   	        	System.out.println(result.b);
   	        	kvs1.put(outputTable, result.a, row.key(), result.b);
   	        }
   		 }
   	    response.status(200, "OK");
   	    return "OK";
      	
      });
    
    post("/rdd/p2pflatMapToPair", (request, response) -> {
      	 String inputTable = request.queryParams("inputTable");
   		 String outputTable = request.queryParams("outputTable");
   		 String kvsCoordinator = request.queryParams("kvsCoordinator");
   		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
   		 String keyRange1 = request.queryParams("keyRangeFrom");
   		 String keyRange2 = request.queryParams("keyRangeTo");
   		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
   		 FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
   		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
   		 System.out.println(keyRange1);
   		 System.out.println(keyRange2);
   		 if (keyRange1.equals("null")) {
   			 keyRange1=null;
   		 }
   		 if (keyRange2.equals("null")) {
   			 keyRange2=null;
   		 }
   		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
   		 
   		 if (!Iter.hasNext()) {
   			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
   		 }
   		 while (Iter.hasNext()) {
   	        Row row = Iter.next();
//   	        System.out.printf("Row row = Iter.next();");
   	        for(String i:row.columns()) {
   	        	FlamePair fp = new FlamePair(row.key(),row.get(i));
	        	Iterable<FlamePair> result1 = lambda.op(fp);
	 		    Iterator<FlamePair> result = result1.iterator();
	 		   if (result != null) {
		            // Iterate over the elements in the Iterable and put them into the output table
		        	while (result.hasNext()){
		        		FlamePair item =  result.next();
		                // Create a unique row key here to ensure no overwriting
		            	String colKey = UUID.randomUUID().toString();
		                // Store the item in the output table in the KVS
		            	kvs1.put(outputTable, item.a, colKey, item.b);
		            }
		        }
   	        }
   		 }
   	    response.status(200, "OK");
   	    return "OK";
      	
      });
    
    post("/rdd/s2pflatMapToPair", (request, response) -> {
     	 String inputTable = request.queryParams("inputTable");
  		 String outputTable = request.queryParams("outputTable");
  		 String kvsCoordinator = request.queryParams("kvsCoordinator");
  		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
  		 String keyRange1 = request.queryParams("keyRangeFrom");
  		 String keyRange2 = request.queryParams("keyRangeTo");
  		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
  		 FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
  		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
  		 System.out.println(keyRange1);
  		 System.out.println(keyRange2);
  		 if (keyRange1.equals("null")) {
  			 keyRange1=null;
  		 }
  		 if (keyRange2.equals("null")) {
  			 keyRange2=null;
  		 }
  		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
  		 
  		 if (!Iter.hasNext()) {
  			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
  		 }
  		 while (Iter.hasNext()) {
  	        Row row = Iter.next();
//  	        System.out.printf("Row row = Iter.next();");
  	        for(String i:row.columns()) {
	        	Iterable<FlamePair> result1 = lambda.op(row.get(i));
	 		    Iterator<FlamePair> result = result1.iterator();
	 		   if (result != null) {
		            // Iterate over the elements in the Iterable and put them into the output table
		        	while (result.hasNext()){
		        		FlamePair item =  result.next();
		                // Create a unique row key here to ensure no overwriting
		            	String colKey = UUID.randomUUID().toString();
		                // Store the item in the output table in the KVS
		            	kvs1.put(outputTable, item.a, colKey, item.b);
		            }
		        }
  	        }
  		 }
  	    response.status(200, "OK");
  	    return "OK";
     	
     });
    
    post("/rdd/foldByKey", (request, response) -> {
      	 String inputTable = request.queryParams("inputTable");
   		 String outputTable = request.queryParams("outputTable");
   		 String kvsCoordinator = request.queryParams("kvsCoordinator");
   		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
   		 String keyRange1 = request.queryParams("keyRangeFrom");
   		 String keyRange2 = request.queryParams("keyRangeTo");
   		 String zero = request.queryParams("zero");
   		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
   		 FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
   		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
   		 System.out.println(keyRange1);
   		 System.out.println(keyRange2);
   		 if (keyRange1.equals("null")) {
   			 keyRange1=null;
   		 }
   		 if (keyRange2.equals("null")) {
   			 keyRange2=null;
   		 }
   		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
   		 
   		 if (!Iter.hasNext()) {
   			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
   		 }
   		 while (Iter.hasNext()) {
   	        Row row = Iter.next();
//   	        System.out.printf("Row row = Iter.next();");
   	        int j =0;
   	        String result= null;
   	        for(String i:row.columns()) {
   	        	if (j ==0) {
   	        		result = lambda.op(zero,row.get(i));
   	        	}else {
   	        		result = lambda.op(result,row.get(i));
   	        	}
   	        	j=1;
   	        }
   	        kvs1.put(outputTable, row.key(),"value", result);
   		 }
   	    response.status(200, "OK");
   	    return "OK";
      });

    
    post("/rdd/groupBy", (request, response) -> {
     	 String inputTable = request.queryParams("inputTable");
  		 String outputTable = request.queryParams("outputTable");
  		 String kvsCoordinator = request.queryParams("kvsCoordinator");
  		 //int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
  		 String keyRange1 = request.queryParams("keyRangeFrom");
  		 String keyRange2 = request.queryParams("keyRangeTo");
  		 String zero = request.queryParams("zero");
  		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
  		 FlameRDD.StringToString lambda = (FlameRDD.StringToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
  		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
  		 System.out.println(keyRange1);
  		 System.out.println(keyRange2);
  		 if (keyRange1.equals("null")) {
  			 keyRange1=null;
  		 }
  		 if (keyRange2.equals("null")) {
  			 keyRange2=null;
  		 }
  		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
  		 
  		 if (!Iter.hasNext()) {
  			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
  		 }
  		HashMap<String, StringBuilder> vstorage = new HashMap<>();

  	    while (Iter.hasNext()) {
  	        Row row = Iter.next();
  	        String result = null;

  	        // Iterate through columns and apply the lambda function
  	        for (String columnName : row.columns()) {
  	            result = lambda.op(row.get(columnName));

  	            if (vstorage.containsKey(result)) {
  	                vstorage.get(result).append(",").append(row.get(columnName));
  	            } else {
  	                StringBuilder valueList = new StringBuilder();
  	                valueList.append(row.get(columnName));
  	                vstorage.put(result, valueList);
  	            }
  	        }
  	    }

  	    // Write the aggregated values to the output table
  	    for (String key : vstorage.keySet()) {
  	        kvs1.put(outputTable, key, "value", vstorage.get(key).toString());
  	    }
	  	  response.status(200,"OKs");
	      return "OK";
	     	
     });
    
    post("/rdd/fromTable", (request, response) -> {
    	 String inputTable = request.queryParams("inputTable");
 		 String outputTable = request.queryParams("outputTable");
 		 String kvsCoordinator = request.queryParams("kvsCoordinator");
 		 String keyRange1 = request.queryParams("keyRangeFrom");
 		 String keyRange2 = request.queryParams("keyRangeTo");
 		 String zero = request.queryParams("zero");
 		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
 		 FlameContext.RowToString lambda = (FlameContext.RowToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
 		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
 		 System.out.println(keyRange1);
 		 System.out.println(keyRange2);
 		 if (keyRange1.equals("null")) {
 			 keyRange1=null;
 		 }
 		 if (keyRange2.equals("null")) {
 			 keyRange2=null;
 		 }
 		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
 		 
 		 if (!Iter.hasNext()) {
 			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
 		 }


 	    while (Iter.hasNext()) {
 	        Row row = Iter.next();
 	        String result = null;

 	        result = lambda.op(row);
 	        kvs1.put(outputTable, row.key(), "value", result);
 	        }

	  	  response.status(200,"OKs");
	      return "OK";  	
    });
    
    post("/rdd/join", (request, response) -> {
   	 String inputTable = request.queryParams("inputTable");
		 String outputTable = request.queryParams("outputTable");
		 String kvsCoordinator = request.queryParams("kvsCoordinator");
		 String keyRange1 = request.queryParams("keyRangeFrom");
		 String keyRange2 = request.queryParams("keyRangeTo");
		 String other = request.queryParams("zero");
		 System.out.println("kvsCoordinator="+kvsCoordinator +"inputtable="+inputTable+",received key range from "+keyRange1+" to "+keyRange2);
		 //FlameContext.RowToString lambda = (FlameContext.RowToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
		 KVSClient kvs1 = new KVSClient(kvsCoordinator);
		 System.out.println(keyRange1);
		 System.out.println(keyRange2);
		 if (keyRange1.equals("null")) {
			 keyRange1=null;
		 }
		 if (keyRange2.equals("null")) {
			 keyRange2=null;
		 }
		 Iterator<Row> Iter = kvs1.scan(inputTable, keyRange1,keyRange2);
		 
		 if (!Iter.hasNext()) {
			 System.out.println("kvs1.scan(inputTable, keyRange1,keyRange2) return null");
		 }


	    while (Iter.hasNext()) {
	        Row row = Iter.next();
	        Row row2 = kvs1.getRow(other,row.key());
	        for (String key1 : row.columns()) {
	        	for (String key2 : row2.columns()) {
	        		String comb = row.get(key1)+","+row2.get(key2);
	        		String colKey = UUID.randomUUID().toString();
	        		kvs1.put(outputTable, row.key(), colKey, comb);
	        		}
	  	    	}
	        }

	  	  response.status(200,"OKs");
	      return "OK";  	
   });
	 // Decode query parameters
//		 String inputTable = request.queryParams("inputTable");
//		 String outputTable = request.queryParams("outputTable");
//		 String kvsCoordinatorHost = request.queryParams("kvsCoordinatorHost");
//		 int kvsCoordinatorPort = Integer.parseInt(request.queryParams("kvsCoordinatorPort"));
//		 String keyRange = request.queryParams("keyRange");
//	
//	 // Deserialize the lambda
//	 LambdaType lambda = Serializer.deserializeLambda(request.queryParams("lambda"), "your-job.jar");
//	
//	 // Use KVSClient to scan keys in the specified range
//	 KVSClient kvsClient = new KVSClient(kvsCoordinatorHost, kvsCoordinatorPort);
//	 KeyValueIterator iterator = kvsClient.scanKeysInRange(inputTable, keyRange);
//	
//	 // Process the keys using the lambda
//	 while (iterator.hasNext()) {
//	     KeyValue keyValue = iterator.next();
//	     String key = keyValue.getKey();
//	     String value = keyValue.getValue();
//	
//	     // Invoke the lambda function on the value
//	     Iterable<String> result = lambda.apply(value);
//	
//	     // Put the result into the output table, making sure to generate unique row keys
//	     // to avoid overwriting values
//	     for (String item : result) {
//	         String uniqueKey = generateUniqueKey();
//	         kvsClient.put(outputTable, uniqueKey, item);
//	     }
//	 }
//	
//	 return "Operation completed"; // Return a response
	 }

//	private static void post(String string, Route route) {
//		// TODO Auto-generated method stub
//		
//	});
}
