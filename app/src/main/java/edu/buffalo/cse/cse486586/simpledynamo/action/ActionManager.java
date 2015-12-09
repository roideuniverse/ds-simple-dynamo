package edu.buffalo.cse.cse486586.simpledynamo.action;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider;
import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.ClientActionWorker;
import edu.buffalo.cse.cse486586.simpledynamo.action.IAction;

public class ActionManager {

    private static int mCount =0;
    private static ExecutorService mClientExecutor = Executors.newSingleThreadExecutor();
    //private static ExecutorService mServerExecutor = Executors.newSingleThreadExecutor();
    
    private ActionManager() {
    
    }
    
    public static synchronized Future<IAction> executeClientAction(IAction action, SimpleDynamoProvider provider) {
        action.setSeqNo(++mCount);
        ClientActionWorker c = provider.new ClientActionWorker(action);
        Future<IAction> f = mClientExecutor.submit(c);
        return f;
    }
    
    public static synchronized Future<Action> executeServerAction(Action action, SimpleDynamoProvider provider) {
        action.setSeqNo(++mCount);
        Future<Action> f = mClientExecutor.submit(action);
        return f;
    }
}
