package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD{
	String tablename;
	KVSClient kvsClient;
	public FlameRDDImpl(String tableName,KVSClient	kvsClient) {
        this.tablename = tableName;
        this.kvsClient = kvsClient;
    }
	@Override
	public List<String> collect() throws Exception {
//		KVSClient kvsClient = getKVS(); // You can get the KVSClient instance from your context.
        List<String> collectedValues = new ArrayList<>();

        Iterator<Row> iterator = kvsClient.scan(tablename);

        while (iterator.hasNext()) {
            Row row = iterator.next();
            String value = row.get("value");
            if (value != null) {
                collectedValues.add(value);
            }
        }


        return collectedValues;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		// TODO Auto-generated method stub
		FlameContextImpl fc = new FlameContextImpl("flatMap");
		String outputtable = fc.invokeOperation("/rdd/flatMap",Serializer.objectToByteArray(lambda), this.tablename,null);
		return new FlameRDDImpl(outputtable,this.kvsClient);
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		// TODO Auto-generated method stub
		FlameContextImpl fc = new FlameContextImpl("mapToPair");
		String outputtable = fc.invokeOperation("/rdd/mapToPair",Serializer.objectToByteArray(lambda), this.tablename,null);
		return new FlamePairRDDImpl(outputtable,this.kvsClient);
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("groupBy");
		String outputtable = fc.invokeOperation("/rdd/groupBy",Serializer.objectToByteArray(lambda), this.tablename,null);
		return new FlamePairRDDImpl(outputtable,this.kvsClient);
	}
	
	@Override
	public int count() throws Exception {
		// TODO Auto-generated method stub
		return kvsClient.count(tablename);
	}
	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		kvsClient.rename(tablename, tableNameArg);
		return;
		
	}
	@Override
	public FlameRDD distinct() throws Exception {
		Iterator<Row> iterator = kvsClient.scan(tablename);
		String outputtable =  UUID.randomUUID().toString();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String value = row.get("value");
            if (value != null) {
            	this.kvsClient.put(outputtable,value,"value",value);
            }
        }
		return new FlameRDDImpl(outputtable,this.kvsClient);
	}
	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Vector<String> take(int num) throws Exception {
		Vector<String> ans = new Vector<String>();
		Iterator<Row> rowIter = kvsClient.scan(tablename);
		int count = 0;
	    
	    while (rowIter.hasNext() && count < num) {
	        Row row = rowIter.next();
	        count+=1;
	        // Assuming each row contains a 'value' column, you need to retrieve the value.
	        for (String columnName : row.columns()) {
	            String value = row.get(columnName);
	            ans.add(value);
//	            count++;
//
//	            if (count >= num) {
//	                break;
//	            }
	        }
	    }
		return ans;
	}
	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("fold");
		String output = fc.invokeOperation("/rdd/foldByKey",Serializer.objectToByteArray(lambda), this.tablename,zeroElement);
		Iterator<Row> rowIter = kvsClient.scan(output);
	    String res = zeroElement;
	    while (rowIter.hasNext()) {
	        Row row = rowIter.next();
	        System.out.println(row.get("value"));
	        res = lambda.op(res, row.get("value"));
	        System.out.println(res);
	    }
		return res;
	}
	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("s2pflatMapToPair");
		String outputtable = fc.invokeOperation("/rdd/s2pflatMapToPair",Serializer.objectToByteArray(lambda), this.tablename,null);
		return new FlamePairRDDImpl(outputtable,this.kvsClient);
	}
	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
