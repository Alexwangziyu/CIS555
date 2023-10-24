package cis5550.flame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;

public class  FlameContextImpl implements FlameContext {
	public StringBuilder outputString = new StringBuilder();
	// Constructor that takes a parameter
    public  FlameContextImpl(String someParameter) {
    }
    
	@Override
	public KVSClient getKVS() {
		// TODO Auto-generated method stub
		return Coordinator.kvs;
	}

	@Override
	public void output(String s) {
		// TODO Auto-generated method stub
		
		outputString.append(s);
		return;
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception { 
		// Generate a unique table name, for example, using a job ID and a sequence number
        String tableName = "job_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString();

        // Use KVSClient to upload the strings to the KVS with the specified table name
        KVSClient kvsClient = getKVS();
        for (int i = 0; i < list.size(); i++) {
            String rowKey = UUID.randomUUID().toString(); // You can define a hash method or use another method for generating row keys.
            kvsClient.put(tableName, rowKey, "value", list.get(i));
        }

        // Create a new FlameRDD instance and store the table name
        FlameRDDImpl flameRDD = new FlameRDDImpl(tableName,kvsClient);

        return flameRDD;
    }

	public String getOutputStrings() {
	    return this.outputString.toString();
	}
	
	public String invokeOperation(
		    String operationName,
		    byte[] serializedLambda,
		    String inputRDD,
		    String zero
		) {
		 	String outputtableName = "job_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString();
		    Partitioner p = new Partitioner();
		    String Previous = null;
		    int numberkvs=0;
		    try {
		    	numberkvs = getKVS().numWorkers();
		    }catch (IOException e) {
	            e.printStackTrace();
	        }
		    System.out.println(numberkvs);
		    String minim = null;
		    if (numberkvs == 1) {
		    	try {
		    	String address = getKVS().getWorkerAddress(0);
		    	 p.addKVSWorker(address, null, null);
		    	}catch (IOException e) {
		            e.printStackTrace();
		        }
		    	
		    }else {
		    	for (int i = 0; i < numberkvs; i++) {
			        try {
			            String address = getKVS().getWorkerAddress(i);
			            String id = getKVS().getWorkerID(i);
			            System.out.println(address);
			            System.out.println(id);
			            if (i == 0) {
			            	minim= id;
			            }
			            // Calculate the "next" worker's ID or null for the last worker
			            String nextId = (i == numberkvs - 1) ? null : getKVS().getWorkerID(i + 1);
			            p.addKVSWorker(address, id, nextId);
			            if (i == numberkvs - 1) {
			            	p.addKVSWorker(address, null, minim);
			            }
			            
			        } catch (IOException e) {
			            // 处理IOException异常，可以输出错误信息或者采取其他措施
			            e.printStackTrace();
			        }
			    }
		    	
		    }
		    
		    
		    for (int i = 0; i < Coordinator.getWorkers().size(); i++) {
		        System.out.println(Coordinator.getWorkers().elementAt(i));
		        p.addFlameWorker(Coordinator.getWorkers().elementAt(i));
		    }
		    
		    Vector<Partition> result = p.assignPartitions();
		    List<Thread> requestThreads = new ArrayList<>();
		    List<Integer> responseCodes = new ArrayList<>();
		    
		    for (Partition partition : result) {
		        String workerAddress = partition.assignedFlameWorker;
		        System.out.println("workerAddress");
		        System.out.println(workerAddress);
		        System.out.println("kvsWorkerAddress");
		        System.out.println(partition.kvsWorker);
		        String operationURL = "http://" + workerAddress + operationName +
		        	    "?inputTable=" + inputRDD +
		        	    "&outputTable=" + outputtableName +
		        	    "&kvsCoordinator=" + getKVS().getCoordinator() +
		        	    "&keyRangeFrom=" + partition.fromKey +  
		        	    "&keyRangeTo=" + partition.toKeyExclusive  +
		        	    "&zero=" + zero;    

		        Thread requestThread = new Thread(() -> {
		            try {
		                int responseCode = HTTP.doRequest("POST",operationURL, serializedLambda).statusCode();
		                responseCodes.add(responseCode);
		            } catch (IOException e) {
		                // 处理IOException异常，可以输出错误信息或者采取其他措施
		                e.printStackTrace();
		            }
		        });

		        requestThreads.add(requestThread);
		        requestThread.start();
		    }

		    for (Thread requestThread : requestThreads) {
		        try {
		            requestThread.join();
		        } catch (InterruptedException e) {
		            e.printStackTrace();
		        }
		    }

		    boolean hasFailures = false;
		    for (int code : responseCodes) {
		        if (code != 200) {
		            hasFailures = true;
		            break;
		        }
		    }

		    if (hasFailures) {
		        System.out.println("Some requests failed or returned non-200 status codes.");
		    } else {
		        System.out.println("All requests were successful.");
		    }
		    return outputtableName;
		}
			    
}
