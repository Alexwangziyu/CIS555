package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD{
	String tablename;
	KVSClient kvsClient;
	public FlamePairRDDImpl(String tableName,KVSClient	kvsClient) {
        this.tablename = tableName;
        this.kvsClient = kvsClient;
    }
	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> collectedValues = new ArrayList<>();

        Iterator<Row> iterator = kvsClient.scan(tablename);

        while (iterator.hasNext()) {
            Row row = iterator.next();
            for(String i : row.columns()) {
                collectedValues.add(new FlamePair(row.key(),row.get(i)));
            }
        }


        return collectedValues;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("foldByKey");
		String outputtable = fc.invokeOperation("/rdd/foldByKey",Serializer.objectToByteArray(lambda), this.tablename,zeroElement);
		return new FlamePairRDDImpl(outputtable,this.kvsClient);
	}	
	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("p2sflatmap");
		String outputtable = fc.invokeOperation("/rdd/p2sflatmap",Serializer.objectToByteArray(lambda), this.tablename,null);
		return new FlameRDDImpl(outputtable,this.kvsClient);
	}
	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("p2pflatMapToPair");
		String outputtable = fc.invokeOperation("/rdd/p2pflatMapToPair",Serializer.objectToByteArray(lambda), this.tablename,null);
		return new FlamePairRDDImpl(outputtable,this.kvsClient);
	}
	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		FlameContextImpl fc = new FlameContextImpl("join");
		String outputtable = fc.invokeOperation("/rdd/join",null, this.tablename,other. gettablename());
		return new FlamePairRDDImpl(outputtable,this.kvsClient);
	}
	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String gettablename() {
		// TODO Auto-generated method stub
		return tablename;
	}

}
