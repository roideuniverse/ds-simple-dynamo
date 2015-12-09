package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import android.util.Log;
import edu.buffalo.cse.cse486586.simpledynamo.message.IMessage;

public class Worker implements Callable<IMessage> {
    private IMessage mMessage;
    private String TAG = Worker.class.getSimpleName();
    
    Worker(IMessage msg) {
        mMessage = msg;
    }
    @Override
    public IMessage call() throws Exception {
        Socket socket = null;
        try {
            Log.d(TAG, "connecting socket to::" + (mMessage.getDestination()/2));
            socket = new Socket(InetAddress.getByAddress(new byte[] {
                    10, 0, 2, 2
            }), mMessage.getDestination());
            socket.setSoTimeout(Constants.SOCKET_TIMEOUT_HIGH);

            /*
             * Write to socket
             */
            ObjectOutputStream oStream = new ObjectOutputStream(socket.getOutputStream());
            oStream.writeObject(mMessage);
            oStream.flush();

            /*
             * Read from socket.
             */
            ObjectInputStream iStream = new ObjectInputStream(socket.getInputStream());
            IMessage msg = (IMessage) iStream.readObject();
            iStream.close();
            oStream.close();
            socket.close();
            return msg;
        } catch (UnknownHostException e2) {
            e2.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, "exception:" + e + "::" + (mMessage.getDestination()/2));
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
}
