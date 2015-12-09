package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;
import edu.buffalo.cse.cse486586.simpledynamo.action.Action;
import edu.buffalo.cse.cse486586.simpledynamo.action.ActionClientDelete;
import edu.buffalo.cse.cse486586.simpledynamo.action.ActionClientInsert;
import edu.buffalo.cse.cse486586.simpledynamo.action.ActionManager;
import edu.buffalo.cse.cse486586.simpledynamo.action.ActionClientQuery;
import edu.buffalo.cse.cse486586.simpledynamo.action.ActionSyncData;
import edu.buffalo.cse.cse486586.simpledynamo.action.ActionType;
import edu.buffalo.cse.cse486586.simpledynamo.action.IAction;
import edu.buffalo.cse.cse486586.simpledynamo.message.DataRequest;
import edu.buffalo.cse.cse486586.simpledynamo.message.DataResponse;
import edu.buffalo.cse.cse486586.simpledynamo.message.DeleteRequest;
import edu.buffalo.cse.cse486586.simpledynamo.message.IMessage;
import edu.buffalo.cse.cse486586.simpledynamo.message.InsertMessage;
import edu.buffalo.cse.cse486586.simpledynamo.message.MessageType;
import edu.buffalo.cse.cse486586.simpledynamo.message.QueryRequest;
import edu.buffalo.cse.cse486586.simpledynamo.message.QueryResponse;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    
    public static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    
    private static DynamoDataBase mDatabase;
    private static String mLocalNodeId;
    private static String mLocalNodeName;
    private SimpleDynamoProvider mThis;
    private int mHost;
    public static boolean serverisSyncingData = false;    
    private static ConcurrentHashMap<String,ExecutorService> mMapExec = new ConcurrentHashMap<String, ExecutorService>();
    private static ExecutorService mSyncExecutor = Executors.newSingleThreadExecutor();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
	    ActionClientDelete del = new ActionClientDelete(selection);
	    ActionManager.executeClientAction(del, this);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
	    String key = values.getAsString(Constants.KEY);
	    String value = values.getAsString(Constants.VALUE);
	    Log.d(TAG, "insert()::key=" + key + "::value=" + value);
	    
	    ActionClientInsert ins = new ActionClientInsert(key, value);
	    ActionManager.executeClientAction(ins, this);
	    
		return null;
	}

	@Override
	public boolean onCreate() {
	    Log.d(TAG, "Provider::onCreate()");
	    return init();
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
	    Log.d(TAG, "query()::key="+selection);
		ActionClientQuery que = new ActionClientQuery(selection);
		Future<IAction> f =  ActionManager.executeClientAction(que, this);
        try {
            ActionClientQuery res = (ActionClientQuery) f.get();
            Cursor cv = res.getQueryResultAsCursor();
            Log.d(TAG, "for key=" + selection + " count=" + cv.getCount() + " result=" + res.getQueryResultAsMap());
            return cv;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}
	
	boolean init() {
	    mThis = this;
        mDatabase = DynamoDataBase.getHandle(getContext());
        String emulatorName = Util.getEmulatorName(getContext());
        Log.d(TAG, "emulator:" + emulatorName);

        String nodeId = null;
        nodeId = Util.genHash(emulatorName);
        Log.d(TAG, "nodeid:" + nodeId);

        if (nodeId == null) {
            Log.d(TAG, "NodeId:" + nodeId);
            return false;
        }
        mLocalNodeId = nodeId;
        mLocalNodeName = emulatorName;
        mHost = Constants.MAP_NODENAME_PORT.get(mLocalNodeName);
        
        new ServerTask().execute();
        Log.d(TAG, "init()::AskForData--Initiated..");
        askForData();
        Log.d(TAG, "init()::AskForData--Completed");
        
        return true;
	}
	
	private void askForData() {
	    Log.d(TAG, "askForData()::");
	    ActionSyncData sync = new ActionSyncData(); 
	    try {
            //ActionManager.executeClientAction(sync, this).get();
	        ClientActionWorker worker = new ClientActionWorker(sync);
	        mSyncExecutor.submit(worker).get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    serverisSyncingData = false;
	}
    
    /*
     * 
     * 
     * 
     * Action Worker
     * 
     * 
     * 
     * 
     */
    public class ClientActionWorker implements Callable<IAction> {
        private String TAG = ClientActionWorker.class.getSimpleName();
        IAction mAction;
        int mActionCode = 0;
        public ClientActionWorker(IAction action) {
            mAction = action;
            mActionCode = mAction.hashCode();
        }

        @Override
        public IAction call() throws Exception {
            Log.d(TAG, "ActionClientWorker()::" + mAction.getType() + " ActionHash=" + mActionCode);
            boolean wasHeld = false;
            while(serverisSyncingData) {
                Log.d(TAG, "ClientActionWorker::WaitingForSyncToComplete::" + mAction.getType() + "::" +  mActionCode);
                wasHeld = true;
                Thread.sleep(Constants.SLEEP_TIME);
            }
            if(wasHeld) 
                Log.d(TAG, "ActionClientWorker()::Recovering:" + mAction.getType() + " ActionHash=" + mActionCode);
            
            if(mAction.getType() == ActionType.DATA_SYNC) {
                handleDataSync(mAction);
            } else if(mAction.getType() == ActionType.CLIENT_INSERT) {
                handleClientInsert(mAction);
            } else if(mAction.getType() == ActionType.CLIENT_QUERY) {
                return handleClientQuery(mAction);
            } else if(mAction.getType() == ActionType.CLIENT_DELETE) {
                handleClientDelete(mAction);
            } else 
                throw new IllegalStateException("Unknown Action Command");
            return null;
        }
        
        private void handleClientInsert(IAction action) {
            Log.d(TAG, "handleClientInsert()::" + action.getType() + "::" + mActionCode );
            ActionClientInsert ai = (ActionClientInsert) action;
            Log.d(TAG, "handleClientInsert()::key="+ai.getKey() + " " + mActionCode);
            
            if(Util.iskeyLocal(ai.getKey(), mLocalNodeId)) {
                mDatabase.putValue(Util.createCV(ai.getKey(), ai.getValue()));
                String succName = Util.getSuccessorName(mLocalNodeName);
                String secSuccName = Util.getSuccessorName(succName);
                dispatchInsertMessage(succName, ai);
                dispatchInsertMessage(secSuccName, ai);
                return;
            }
            String parent = Util.getParentName(ai.getKey());
            String succName = Util.getSuccessorName(parent);
            String secSuccName = Util.getSuccessorName(succName);
            dispatchInsertMessage(parent, ai);
            dispatchInsertMessage(succName, ai);
            dispatchInsertMessage(secSuccName, ai);
        }
        
        private void dispatchInsertMessage(String destName, ActionClientInsert ai) {
            Log.d(TAG, "dispatchInsertMessage()::to:" + destName + " key=" + ai.getKey() + " " + mActionCode);
            InsertMessage msg = new InsertMessage();
            msg.setHost(mHost);
            msg.setDestination(Constants.MAP_NODENAME_PORT.get(destName));
            msg.setPayload(Util.createMap(ai.getKey(), ai.getValue()));
            if(destName.equals(mLocalNodeName)) {
                mDatabase.putValue(Util.createCV(ai.getKey(), ai.getValue()));
                return;
            }
            Socket s = createSocket(msg.getDestination(), Constants.SOCKET_TIMEOUT_LESS);
            writeToSocket(s, msg);
        }
        
        private Action handleClientQuery(IAction action) {
            Log.d(TAG, "handleClientQuery:" + mActionCode);
            ActionClientQuery aq = (ActionClientQuery) action;
            Log.d(TAG, "handleClientQuery()::key=" + aq.getKey() + " " + mActionCode);
            String key = aq.getKey();
            if(key.equals("@")) {
                Cursor cv = mDatabase.getValue(key);
                aq.setQueryResult(Util.CursorToMap(cv));
                return aq;
            }

            HashMap<String,String> payload = new HashMap<String,String>();
            if(key.equals("*")) {
                payload.putAll(Util.CursorToMap(mDatabase.getValue("@")));
                for(String nodeName:Constants.MAP_NODENAME_PORT.keySet()) {
                    if(!nodeName.equals(mLocalNodeName)) {
                        QueryResponse r = dispatchQueryRequest(nodeName, "@");
                        if(r!=null) {
                            payload.putAll(r.getPayload());
                        }
                    }
                }
            } else {
                String parent = Util.getParentName(key);
                String succParent = Util.getSuccessorName(parent);
                QueryResponse r = dispatchQueryRequest(parent, key);
                if(r==null) {
                    r = dispatchQueryRequest(succParent, key);
                }
                if(r!=null) {
                    payload.putAll(r.getPayload());
                } else if(r==null ) {
                    Log.d(TAG, "handleClientQuery:" + mActionCode + "::response from both null. Retying" );
                    r = dispatchQueryRequest(parent, key);
                    if(r==null) {
                        r = dispatchQueryRequest(succParent, key);
                    }
                    if(r!=null) {
                        payload.putAll(r.getPayload());
                    }
                    Log.d(TAG, "handleClientQuery:" + mActionCode + " retry response=" + r);
                }
            }
            aq.setQueryResult(payload);
            return aq;
        }
        
        private QueryResponse dispatchQueryRequest(String destName, String key) {
            Log.d(TAG, "dispatchQueryRequest()::key=" + key + "::destination::" + destName + "" + mActionCode);
            int dest = Constants.MAP_NODENAME_PORT.get(destName);
            
            if(destName.equals(mLocalNodeName)) {
                Log.d(TAG, "key=" + key + "::isLocal.FetchingFromDb" + "");
                QueryResponse res = new QueryResponse();
                Cursor cv = mDatabase.getValue(key);
                res.addPayload(Util.CursorToMap(cv));
                return res;
            }
            
            if(dest == Constants.MAP_NODENAME_PORT.get(mLocalNodeName)) {
                throw new IllegalStateException("dispatchQueryRequest::wtiting to its own socket");
            }
            
            QueryRequest qReq = new QueryRequest();
            qReq.setQueryString(key);
            qReq.setHost(mHost);
            
            Socket socket = createSocket(dest, Constants.SOCKET_TIMEOUT_HIGH);
            if(socket == null) 
                return null;
            try {
                Log.d(TAG, "QueryReq, writing to socket for key="+ key + "::dest=" + (dest/2) + "::connection=" + socket.isConnected() );
                ObjectOutputStream oStream = new ObjectOutputStream(socket.getOutputStream());
                oStream.writeObject(qReq);
                oStream.flush();
                
                ObjectInputStream iStream = new ObjectInputStream(socket.getInputStream());
                QueryResponse msg = (QueryResponse) iStream.readObject();
                iStream.close();
                oStream.close();
                socket.close();
                return msg;
                
            } catch (IOException e) {
                Log.e(TAG, "dispatchQueryRequest()::Exception key=" + key + " dest=" + (dest/2) + "::" + e);
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
        
        private void handleClientDelete(IAction action) {
            Log.d(TAG, "handleClientDelete()::" + mAction);
            ActionClientDelete ad = (ActionClientDelete) action;
            Log.d(TAG, "handleClientDelete()::key=" + ad.getKey() + " " + mActionCode);
            String key = ad.getKey();
            if(key.equals("@")) {
                mDatabase.delete(key);
                return;
            }
            
            ArrayList<String> dest = new ArrayList<String>();
            if(key.equals("*")) {
                mDatabase.delete("@");
                for(String nodeName:Constants.MAP_NODENAME_PORT.keySet()) {
                    if(!nodeName.equals(mLocalNodeName)) {
                        dest.add(nodeName);
                    }
                }
            } else {
                String parent = Util.getParentName(key);
                String succParent = Util.getSuccessorName(parent);
                dest.add(parent);
                dest.add(succParent);
                dest.add(Util.getSuccessorName(succParent));
            }
            
            for(String nodeName:dest) {
                dispatchDelMessage(nodeName, ad);
            }
        }

        private void dispatchDelMessage(String destName, ActionClientDelete ad) {
            Log.d(TAG, "handleClientDelete()::key=" + ad.getKey() + "::destination::" + destName + " " + mActionCode);
            int dest = Constants.MAP_NODENAME_PORT.get(destName);
            DeleteRequest req = new DeleteRequest();
            req.setHost(mHost);
            req.setDestination(dest);
            req.setDeleteKey(ad.getKey());
            if(destName.equals(mLocalNodeName)) {
                mDatabase.delete(ad.getKey());
                return;
            }
            Socket s = createSocket(dest, Constants.SOCKET_TIMEOUT_LESS);
            writeToSocket(s, req);
        }
        
        private void handleDataSync(IAction action) {
            Log.d(TAG, "handleDataSync()::" + mActionCode);
            serverisSyncingData = true;
            String pred = Util.getPredecessorName(mLocalNodeName);
            String secPred = Util.getPredecessorName(pred);
            String succ = Util.getSuccessorName(mLocalNodeName);
            String secSucc = Util.getSuccessorName(succ);
            
            DataResponse secPredRes = null;
            DataResponse predRes = null;
            DataResponse succRes = null;
            
            secPredRes = sendDataSyncRequest(secPred, new String[] {secPred});
            if(secPredRes==null) {
                Log.d(TAG, "handleDataSync()::failed::for:secPred:"+secPred + "::" + mActionCode);
                predRes = sendDataSyncRequest(pred, new String[] {pred, secPred});
            } else {    
                Log.d(TAG, "handleDataSync()::success::for::secPred:"+secPred + "::" + mActionCode);
                predRes = sendDataSyncRequest(pred, new String[] {pred});
            }
            
            if(secPredRes == null && predRes == null) {
                Log.d(TAG, "Both secPredRes and PredRes null. Not possible" + "::" + mActionCode);
                serverisSyncingData = false;
                //throw new IllegalStateException("Both secPredRes and PredRes null. Not possible" + "::" + mActionCode);
                Log.d(TAG, "Both secPredRes and PredRes null. Not possible" + "::" + mActionCode);
            }
            
            if(predRes == null) {
                Log.d(TAG, "handleDataSync()::failed:for:pred:" + pred + "::" + mActionCode);
                succRes = sendDataSyncRequest(succ, new String[] {pred, mLocalNodeName});
            } else {
                Log.d(TAG, "handleDataSync()::success:for:pred:" + pred + "::" + mActionCode);
                succRes = sendDataSyncRequest(succ, new String[] {mLocalNodeName});
            }
            
            if(succRes == null) {
                Log.d(TAG, "handleDataSync()::failed:for:succ:" + succ + "::" + mActionCode);
                succRes = sendDataSyncRequest(secSucc, new String[] {mLocalNodeName});
            }
            
            /*predRes = SendDataSyncRequest(pred, new String[] { pred, secPred});
            
            if(predRes == null) {
                Log.e(TAG, "handleDataSync()::failed::for:pred:" + pred);
                
                secPredRes = SendDataSyncRequest(secPred, new String[] {secPred});
                if(secPredRes == null) {
                    Log.e(TAG, "handleDataSync()::failed::for:secPred:"+ secPred);
                    Log.e(TAG, "Two failues--Find out the reason..(pred,secPred)");
                }
                
                succRes = SendDataSyncRequest(succ, new String[] {mLocalNodeName, pred});
                if(succRes == null) {
                    Log.e(TAG, "handleDataSync()::failed::for:succ:"+ succ);
                    Log.e(TAG, "Two failues--Find out the reason..(pred,succ)");
                }
            } else {
                //contact succ or secondarySuccessor for my data
                succRes = SendDataSyncRequest(succ, new String[] {mLocalNodeName});
                if(succRes == null) {
                    Log.d(TAG, "handleDataSync()::failed:for:successor::" + succ);
                    succRes = SendDataSyncRequest(secSucc, new String[] {mLocalNodeName});
                    Log.d(TAG, "handleDataSync()::failed:for:secondarysuccessor::" + secSucc);
                }
            }*/
            
            //HashMap<String,ArrayList<String>> dataPayload = new HashMap<String,ArrayList<String>>();
            if(secPredRes != null) {
                Map<String,ArrayList<String>> payload = secPredRes.getPayload();
                Log.d(TAG, "handleDataSync()::receivedFrom:secPred:" + payload + "::" + mActionCode);
                for(String key:payload.keySet()) {
                    ArrayList<String> value = payload.get(key);
                    mDatabase.putValue(this.createCVWithVersion(key, value));
                }
            }
            if(predRes != null) {
                Map<String,ArrayList<String>> payload = predRes.getPayload();
                Log.d(TAG, "handleDataSync()::receivedFromPred:" + payload + "::" + mActionCode);
                for(String key:payload.keySet()) {
                    ArrayList<String> value = payload.get(key);
                    mDatabase.putValue(this.createCVWithVersion(key, value));
                }
            }
            if(succRes != null) {
                Map<String,ArrayList<String>> payload = succRes.getPayload();
                Log.d(TAG, "handleDataSync()::receivedFromSucc:" + payload + "::" + mActionCode);
                for(String key:payload.keySet()) {
                    ArrayList<String> value = payload.get(key);
                    mDatabase.putValue(this.createCVWithVersion(key, value));
                }
            }
            Log.d(TAG, "Sync is over..!!!");
            serverisSyncingData = false;
        }
        
        private ContentValues createCVWithVersion(String key, ArrayList<String> value) {
            ContentValues cv = new ContentValues();
            cv.put(Constants.KEY, key);
            cv.put(Constants.VALUE, value.get(0));
            cv.put(Constants.VERSION, value.get(1));
            return cv;
        }
        
        private DataResponse sendDataSyncRequest(String destName, String[] nodeNameList) {
            Log.d(TAG, "SendDataSyncRequest()::" + destName + " " + mActionCode);
            int dest = Constants.MAP_NODENAME_PORT.get(destName);
            DataRequest dReq = new DataRequest();
            dReq.setHost(mHost);
            for(int i=0;i<nodeNameList.length;i++) {
                dReq.addNodeName(nodeNameList[i]);
            }
            
            Socket socket = createSocket(dest, Constants.SOCKET_TIMEOUT_AVERAGE);
            try {
                if(socket == null)
                    return null;
                Log.d(TAG, "SendDataSyncRequest::" + "::dest=" + (dest/2) + "::connection=" + socket.isConnected() );
                ObjectOutputStream oStream = new ObjectOutputStream(socket.getOutputStream());
                oStream.writeObject(dReq);
                oStream.flush();
                
                ObjectInputStream iStream = new ObjectInputStream(socket.getInputStream());
                DataResponse msg = (DataResponse) iStream.readObject();
                iStream.close();
                oStream.close();
                socket.close();
                return msg;
            } catch (IOException e) {
                Log.e(TAG, "SendDataSyncRequest::Exception::dest=" + (dest/2) + "::" + e);
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
        
    /*
     * SERVER
     * 
     * Action Handle Connection
     * 
     */
    private class ServerTask extends AsyncTask<ServerSocket, IMessage, Void> {
        private static final String TAG = "ServerTask";
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.d(TAG, "ServerTask:doInBackground");
            ServerSocket serverSocket = null;               
            
            //ExecutorService execServ = Executors.newFixedThreadPool(2);
            ExecutorService execServ = Executors.newCachedThreadPool();
            //ExecutorService execServ = Executors.newSingleThreadExecutor();
            try {
                serverSocket = new ServerSocket(Constants.SERVER_PORT);
                Log.d(TAG, "ServerSocket Created!");
                while (mThis != null) {
                    Socket client = serverSocket.accept();
                    ActionHandleProxy eConn = new ActionHandleProxy(client);
                    execServ.submit(eConn);
                }
                serverSocket.close();
            } catch (IOException e) {
                Log.d(TAG, "Exception:doInBackground:" + e.toString());
                e.printStackTrace();
            }
            return null;
        }
        @Override
        protected void onProgressUpdate(IMessage... message) {
            IMessage msg = message[0];
            Log.d(TAG, "onProgressUpdate::" + msg.getType());
        }
    }

    public class ActionHandleProxy implements Runnable {
        private Socket mServerSocket;
        private int mThreadCode;
        
        public ActionHandleProxy(Socket socket) {
            mServerSocket = socket;
            mThreadCode = this.hashCode();
        }
        
        @Override
        public void run() {
            try {
                ObjectInputStream iStream = new ObjectInputStream(mServerSocket.getInputStream());
                IMessage msg = (IMessage) iStream.readObject();
                Log.d(TAG, "Proxy:Received:" + msg.getType() + " from=" + (msg.getHost() / 2) + " tCode=" + mThreadCode);

                if (msg.getType() == MessageType.VALUE_INSERT_REQUEST) {
                    InsertMessage m = (InsertMessage) msg;
                    Log.d(TAG, "Proxy:insert::for::" + m.getPayload() + " tCode=" + mThreadCode);
                    for (String key : m.getPayload().keySet()) {
                        ExecutorService e = null;
                        if(mMapExec.containsKey(key)) {
                            Log.d(TAG, "containsKey:" + key + " tCode=" + mThreadCode);
                            e = mMapExec.get(key);
                        } else {
                            Log.d(TAG, "new executor for key" + key + " tCode=" + mThreadCode);
                            e = Executors.newSingleThreadExecutor();
                            mMapExec.put(key, e);
                        }
                        ActionHandleConnection c = new ActionHandleConnection(mServerSocket, msg);
                        e.submit(c);
                    }
                } else if (msg.getType() == MessageType.VALUE_DELETE_REQUEST) {
                    DeleteRequest r = (DeleteRequest) msg;
                    Log.d(TAG, "Proxy:del:for::" + r.getDeleteKey() + " tCode=" + mThreadCode);
                    String key = r.getDeleteKey();
                    ExecutorService e = null;
                    if(mMapExec.containsKey(key)) {
                        Log.d(TAG, "containsKey:" + key +" tCode=" + mThreadCode);
                        e = mMapExec.get(key);
                    } else {
                        Log.d(TAG, "new executor for key" + key  + " tCode=" + mThreadCode);
                        e = Executors.newSingleThreadExecutor();
                        mMapExec.put(key, e);
                    }
                    ActionHandleConnection c = new ActionHandleConnection(mServerSocket, msg);
                    e.submit(c);
                } else if (msg.getType() == MessageType.VALUE_QUERY_REQUEST) {
                    QueryRequest r = (QueryRequest) msg;
                    Log.d(TAG, "Proxy:query::for::" + r.getQueryKey() + " tCode=" + mThreadCode);
                    String key = r.getQueryKey();
                    ExecutorService e = null;
                    if(mMapExec.containsKey(key)) {
                        Log.d(TAG, "containsKey:" + key + " tCode=" + mThreadCode);
                        e = mMapExec.get(key);
                    } else {
                        Log.d(TAG, "new executor for key" + key  + " tCode=" + mThreadCode);
                        e = Executors.newSingleThreadExecutor();
                        mMapExec.put(key, e);
                    }
                    ActionHandleConnection c = new ActionHandleConnection(mServerSocket, msg);
                    e.submit(c);
                } else if (msg.getType() == MessageType.DATA_REQUEST) {
                    ActionHandleConnection c = new ActionHandleConnection(mServerSocket, msg);
                    Log.d(TAG, "Proxy:sync::adding task to sync sxecutor.." + " tCode=" + mThreadCode);
                    mSyncExecutor.submit(c);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
  
    }
        
    public class ActionHandleConnection extends Action {

        private Socket mServerSocket;
        private String TAG = ActionHandleConnection.class.getSimpleName();
        private IMessage mMessage;
        private int mHandleCode;
        
        public ActionHandleConnection(Socket socket, IMessage msg) {
            //Log.d(TAG, "ActionHandleConnection()::"+ socket.getInetAddress() + "" + socket.getPort());
            mServerSocket = socket;
            mMessage = msg;
            mHandleCode = this.hashCode();
        }

        @Override
        public ActionType getType() {
            return ActionType.HANDLE_CONNECTION;
        }

        @Override
        public Action call() throws Exception {
            IMessage msg = mMessage;
            Log.d(TAG, "Received:" + msg.getType() + " from=" + (msg.getHost()/2) + "::" + mHandleCode);
            
            int count = 0;
            while(serverisSyncingData) {
                Log.d(TAG, "ActionHandleConnection::WaitingForSyncToComplete:" + msg.getType() + " from=" + (msg.getHost()/2) +"::" + mHandleCode);
                Thread.sleep(Constants.SLEEP_TIME);
                count++;
            }
            if(count>0) {
                Log.d(TAG, "Recovering Thread " + "::" + mHandleCode);
            }
            
            if(msg.getType() == MessageType.DATA_REQUEST) {
                Log.d(TAG, "serving Sync req from::" +(msg.getHost()/2) + "::" + mHandleCode);
                serverisSyncingData = true;
                DataResponse res = handleDataSyncRequest(msg);
                res.setDestination(msg.getHost());
                writeToSocket(mServerSocket, res);
                Log.w(TAG, "serving Sync completed from::" +(msg.getHost()/2) + "::" + mHandleCode);
                return null;
            }
            if(msg.getType() == MessageType.VALUE_INSERT_REQUEST) {
                InsertMessage m = (InsertMessage) msg;
                Log.d(TAG, "insert:for::" + m.getPayload() + "::" + mHandleCode);
                for(String key:m.getPayload().keySet()) {
                    String val = m.getPayload().get(key);
                    mDatabase.putValue(Util.createCV(key, val));
                    if(mMapExec.contains(key))
                        mMapExec.remove(key);
                }
                
            } else if(msg.getType() == MessageType.VALUE_DELETE_REQUEST) {
                DeleteRequest r = (DeleteRequest) msg;
                Log.d(TAG, "delete:for::" + r.getDeleteKey() + "::" + mHandleCode);
                mDatabase.delete(r.getDeleteKey());
                if(mMapExec.contains(r.getDeleteKey()))
                    mMapExec.remove(r.getDeleteKey());
                
            } else if(msg.getType() == MessageType.VALUE_QUERY_REQUEST) {
                QueryRequest r = (QueryRequest) msg;
                Log.d(TAG, "query:for::" + r.getQueryKey() + "::" + mHandleCode);
                Cursor cv = mDatabase.getValue(r.getQueryKey());
                if(cv.getCount()<1) {
                    Log.d(TAG, "ActionHandleConnection()::cvCount=" + cv.getCount() + " sleeping..20ms waiting for key=" + r.getQueryKey() + "::" + mHandleCode);
                    Log.e(TAG, "Result 0 key=" + r.getQueryKey() + "::" + mHandleCode);
                    throw new IllegalStateException("Result count zero" + "::" + mHandleCode);
                }
                QueryResponse re = new QueryResponse();
                re.setDestination(msg.getHost());
                re.addPayload(Util.CursorToMap(cv));
                Log.d(TAG, "ActionHandleConnection()::key=" + r.getQueryKey() + " values=" + re.getPayload() + "::" + mHandleCode);
                writeToSocket(mServerSocket, re);
                if(mMapExec.contains(r.getQueryKey()))
                    mMapExec.remove(r.getQueryKey());
            } 
            return null;
        }
    }
    
    private void writeToSocket(Socket client, IMessage iMsg) {
        Log.d(TAG, "writeToSocket()::" + (iMsg.getDestination()/2) + "::" + iMsg.getType() );
        
        if(iMsg.getDestination() == Constants.MAP_NODENAME_PORT.get(mLocalNodeName)) {
            Log.e(TAG, "createSocket::wtiting to its own socket");
            return;
        }
        
        ObjectOutputStream oStream;
        try {
            oStream = new ObjectOutputStream(client.getOutputStream());
            oStream.writeObject(iMsg);
            oStream.flush();
            oStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private Socket createSocket(int dest, int time) {
        Log.d(TAG, "connecting socket to::" + (dest/2) + "::local=" + Constants.MAP_NODENAME_PORT.get(mLocalNodeName));        
        Socket socket;
        if(dest == Constants.MAP_NODENAME_PORT.get(mLocalNodeName)) {
            Log.e(TAG, "createSocket::creating sock to itself");
            return null;
        }
        
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), dest);
            socket.setSoTimeout(time);
            return socket;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;   
    }
    
    private DataResponse handleDataSyncRequest(IMessage msg) {
        Log.d(TAG, "handleDataSyncRequest()::from:" + (msg.getHost()/2));
        serverisSyncingData = true;
        DataRequest req = (DataRequest) msg;
        Cursor cursor = mDatabase.getValueWithVersion("@");
        Map<String,ArrayList<String>> data = new HashMap<String, ArrayList<String>>();
        
        if (cursor.moveToFirst()) {
            while (!cursor.isAfterLast()) {
                String key = cursor.getString(cursor.getColumnIndex(Constants.KEY));
                String val = cursor.getString(cursor.getColumnIndex(Constants.VALUE));
                String ver = Integer.toString( cursor.getInt(cursor.getColumnIndex(Constants.VERSION)) );
                data.put(key, new ArrayList(Arrays.asList(new String[] {val, ver} )));
                cursor.moveToNext();
            }
        }
        cursor.close();
        
        HashMap<String,ArrayList<String>> payload = new HashMap<String,ArrayList<String>>();
        for(String node:req.getRequestDataNodeName()) {
            for(String key:data.keySet()) {
                ArrayList<String> value = data.get(key);
                if(Util.iskeyLocal(key, Util.genHash(node))) {
                    payload.put(key, value);
                }
            }
        }
        
        DataResponse res = new DataResponse();
        res.setDestination(req.getHost());
        res.setPayload(payload);
        Log.d(TAG, "handleDataSyncRequest():done:from:"+ (msg.getHost()/2) + "payload=" + payload);
        serverisSyncingData = false;
        return res;
        
    }
    
}
