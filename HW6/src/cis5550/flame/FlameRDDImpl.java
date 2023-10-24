package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


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

}
