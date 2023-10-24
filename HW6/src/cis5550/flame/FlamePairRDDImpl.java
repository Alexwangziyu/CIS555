package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

}
