package edu.buffalo.cse.cse486586.simpledynamo;

import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
    private static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    Button mBtGet;
    Button mBtPut;
    Button mBtDel;
    EditText mEtKey;
    EditText mEtVal;
    
    ContentResolver mContentResolver;
    Uri mUri;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		//Log.d(TAG, "SimpleDynamoActivity()::onCreate");

		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        mContentResolver = getContentResolver();
        
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        
        mBtGet = (Button) findViewById(R.id.bt_get);
        mBtPut = (Button) findViewById(R.id.bt_put);
        mBtDel = (Button) findViewById(R.id.bt_del);
        mEtKey = (EditText) findViewById(R.id.et_key_id);
        mEtVal = (EditText) findViewById(R.id.et_value_id);
        
        mBtGet.setOnClickListener(new OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.d(TAG, "get");
                String key = mEtKey.getText().toString();
                mContentResolver.query(mUri, null, key, null,null);
                //new Task().execute(new String("GET"), new String(key));
            }
        });
        
        mBtPut.setOnClickListener(new OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.d(TAG, "put");
                String key = mEtKey.getText().toString();
                String val = mEtVal.getText().toString();
                ContentValues cv = new ContentValues();
                cv.put(Constants.KEY, key);
                cv.put(Constants.VALUE, val);
                mContentResolver.insert(mUri, cv);
                //new Task().execute(new String("PUT"), new String(key), new String(val));
            }
        });
        
        mBtDel.setOnClickListener(new OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.d(TAG, "del");
                String key = mEtKey.getText().toString();
                String val = mEtVal.getText().toString();
                mContentResolver.delete(mUri, key, null);
                //new Task().execute(new String("DEL"), new String(key), new String(val));
            }
        });
        
        
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    
    private class Task extends AsyncTask<String, String, Void> {
        @Override
        protected Void doInBackground(String... params) {
            Log.d("TAASK", "doInBackground");
            String type = params[0];
            if(type.equals("GET")) {
                String key = params[1];
                mContentResolver.query(mUri, null, key, null,null);
            } else if( type.equals( "PUT")) {
                ContentValues cv = new ContentValues();
                String key = params[1];
                String value = params[2];
                cv.put(Constants.KEY, key);
                cv.put(Constants.VALUE, value);
                mContentResolver.insert(mUri, cv);
            } else if (type.equals("DEL")) {
                String key = params[1];
                mContentResolver.delete(mUri, key, null);
            }
            return null;
        }
        
        protected void onProgressUpdate(String...strings) {
            return;
        }
    }

}
