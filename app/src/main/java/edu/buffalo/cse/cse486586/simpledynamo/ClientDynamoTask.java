package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import android.util.Log;
import edu.buffalo.cse.cse486586.simpledynamo.message.IMessage;

public class ClientDynamoTask {
    
    private static String TAG = ClientDynamoTask.class.getSimpleName();
    private static HashMap<Integer, ExecutorService> mExecutorMap = new HashMap<Integer, ExecutorService>();
    
    private ClientDynamoTask() {
    }
    
    public static Future<IMessage> execute(IMessage msg) {
        ExecutorService executor = null;
        /*if (mExecutorMap.containsKey(msg.getDestination())) {
            executor = mExecutorMap.get(msg.getDestination());
        } else {*/
            executor = Executors.newSingleThreadExecutor();
            /*mExecutorMap.put(msg.getDestination(), executor);
        }
        if (executor == null || executor.isTerminated()) {
            Log.e(TAG, "execute()::could not get executor");
            return null;
        }*/

        Worker c = new Worker(msg);
        Future<IMessage> f = executor.submit(c);
        return f;
    }
}
