package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

public class DynamoDataBase {
    private static final String LOG_TAG = DynamoDataBase.class.getName();

	private static final String COL_KEY = Constants.KEY;
	private static final String COL_VALUE = Constants.VALUE;
	private static final String COL_VERSION = Constants.VERSION;
	private static final int DATABASE_VERSION = 1;
	//private static final String DATABASE_NAME = "dynamo.db";
	private static final String DATABASE_NAME = null;
    private static final String GM_TABLE_NAME = "simple_dynamo";
    private static final String GM_TABLE_CREATE = "CREATE TABLE "
            + GM_TABLE_NAME + " (" + COL_KEY + " TEXT PRIMARY KEY, "
            + COL_VALUE + " TEXT, " + COL_VERSION + " INTEGER default 0 );";
	
	private GMessengerDbHelper mDbHelper;
	private static DynamoDataBase mDynamoDataBase;
	
	private DynamoDataBase(Context context) {
	    mDbHelper = new GMessengerDbHelper(context);
    }

    public static synchronized DynamoDataBase getHandle(Context context) {
        if (mDynamoDataBase == null) {
            mDynamoDataBase = new DynamoDataBase(context);
        }
        return mDynamoDataBase;
    }

    public Cursor getValue(String key) {
        synchronized (mDynamoDataBase) {
            return query(key, false);
        }
    }
    
    public Cursor getValueWithVersion(String key) {
        synchronized (mDynamoDataBase) {
            return query(key, true);
        }
    }

    public int delete(String key) {
        synchronized (mDynamoDataBase) {
            SQLiteDatabase db = mDbHelper.getWritableDatabase();

            if (key.equals("@")) {
                int c = db.delete(mDbHelper.getTableName(), null, null);
                return c;
            }
            int c = db.delete(mDbHelper.getTableName(), Constants.KEY + " = ? ", new String[] {
                key
            });

            return c;
        }
    }

    public Uri putValue(ContentValues values) {
        synchronized (mDynamoDataBase) {
            int existingVersion = 0;
            String key = values.getAsString(Constants.KEY);
            Cursor c = this.queryWithVersion(key);
            
            if(!values.containsKey(COL_VERSION)) {
                if(c.getCount()>0) {
                    if(c.moveToFirst()) {
                        existingVersion = c.getInt(c.getColumnIndex(Constants.VERSION));
                    }
                }
                values.put(COL_VERSION, ++existingVersion);
                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                long id = db.insertWithOnConflict(mDbHelper.getTableName(), null, values,
                        SQLiteDatabase.CONFLICT_REPLACE);
                return Uri.parse("/" + id);
            }
            //now that it contains a version, get the version
            int inputVersion = values.getAsInteger(Constants.VERSION);
            if(c.getCount()>0) {
                if(c.moveToFirst()) {
                    existingVersion = c.getInt(c.getColumnIndex(Constants.VERSION));
                }
            }
            Log.d(LOG_TAG, "existingV=" + existingVersion + " inputV=" + inputVersion);
            if(existingVersion > inputVersion) {
                Log.d(LOG_TAG, "putValue()::Not Inserting");
                return null;
            }
            values.put(COL_VERSION, ++inputVersion);
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            long id = db.insertWithOnConflict(mDbHelper.getTableName(), null, values,
                    SQLiteDatabase.CONFLICT_REPLACE);
            return Uri.parse("/" + id);
        }
    }

    private Cursor query(String selection, boolean version) {
        synchronized (mDynamoDataBase) {
            SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();            
            qBuilder.setTables(mDbHelper.getTableName());
            String[] projectionIn = null;
            if(version) {
                projectionIn = new String[] {COL_KEY, COL_VALUE, COL_VERSION};
            } else {
                projectionIn = new String[] {COL_KEY, COL_VALUE};
            }
            
            if (selection.equals("@")) {
                Cursor c = qBuilder.query(mDbHelper.getReadableDatabase(), projectionIn, null,
                        null, null, null, null);
                Log.d(LOG_TAG, "CursorCount::" + c.getCount());
                return c;
            }

            String selectionClause = COL_KEY + " = ? ";
            String[] selectionArgs = {
                selection
            };

            Cursor c = qBuilder.query(mDbHelper.getReadableDatabase(),
                    projectionIn,
                    selectionClause,
                    selectionArgs,
                    null,
                    null,
                    null);

            return c;
        }
    }
    
    private Cursor queryWithVersion(String selection) {
        synchronized (mDynamoDataBase) {
            SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();            
            qBuilder.setTables(mDbHelper.getTableName());

            String[] projectionIn = {
                    COL_KEY, COL_VALUE, COL_VERSION
            };
            if (selection.equals("@")) {
                Cursor c = qBuilder.query(mDbHelper.getReadableDatabase(), projectionIn, null,
                        null, null, null, null);
                Log.d(LOG_TAG, "CursorCount::" + c.getCount());
                return c;
            }

            String selectionClause = COL_KEY + " = ? ";
            String[] selectionArgs = {
                selection
            };

            Cursor c = qBuilder.query(mDbHelper.getReadableDatabase(),
                    projectionIn,
                    selectionClause,
                    selectionArgs,
                    null,
                    null,
                    null);

            return c;
        }
    }

	public class GMessengerDbHelper extends SQLiteOpenHelper {

		
		//private final Context mDbHelperContext ;
		private SQLiteDatabase mSqliteDatabase;

		public GMessengerDbHelper(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
			//mDbHelperContext = context;
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
		    mSqliteDatabase = db;
		    db.execSQL("DROP TABLE IF EXISTS " + GM_TABLE_NAME);
		    mSqliteDatabase.execSQL(GM_TABLE_CREATE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		    Log.d(LOG_TAG, "Upgrading Database to " + newVersion );
		    //db.execSQL("DROP TABLE IF EXISTS " + GM_TABLE_NAME);
		    onCreate(db);
		}
		
		public String getTableName() {
		    return GM_TABLE_NAME;
		}
		
		public SQLiteDatabase getDataBase() {
            return mSqliteDatabase;
        }

	}

}
