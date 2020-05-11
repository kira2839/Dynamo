

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    public static final Hashtable ringtable = new Hashtable();
    Hashtable dummytable = new Hashtable();
    Hashtable<String, String> hash_table =
            new Hashtable<String, String>();
    public ArrayList<String> replicalist = new ArrayList<String>();
    public static Hashtable<String, String> recoverytable =new Hashtable<String, String>();
    public static final ArrayList<String> orderlist = new ArrayList<String>(5);
    String[] queryvalues={null,null};
    public static boolean firstinsert = true;
    public static boolean firstquery = true;
    public static boolean querynow = false;
    public static boolean recovering = false;
    public boolean isrecovery = false;
    private static Object sharedLock = new Object();
    public boolean restart = true;
  //  public boolean waitforinsert = false;
  //  public boolean waitforquery = false;
  //  public boolean waitforsortinsert = false;
   // public boolean waitforsortquery = false;
  //  public boolean iswaiting = false;
    static final int SERVER_PORT = 10000;
    String[] remoteports= new String[]{"11108","11112","11116","11120","11124"};

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr))*2);
        for (int i = 0; i < orderlist.size(); i++) {
            String queryportall = String.valueOf(ringtable.get(orderlist.get(i)));
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "deleteal", myPort, queryportall);
        }

             //   sqlitedb mydb1 = new sqlitedb(getContext());
        //mydb1.getReadableDatabase().execSQL("delete from "+ "SimpleDynamo");
        //mydb1.getReadableDatabase().execSQL("delete from SimpleDynamo where key" + "= ?", new String[]{selection});
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        ArrayList<String> dummylist = new ArrayList<String>();
        sqlitedb mydb = new sqlitedb(getContext());
        String keyin = values.getAsString("key");
        String valuein = values.getAsString("value");
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myport = String.valueOf((Integer.parseInt(portStr))*2);
      /*  while(waitforinsert){
            Log.v("waitforinsertininsert",String.valueOf(values));
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        waitforinsert = true;*/
        Log.i("startinsertvalues", String.valueOf(values));
        if(values.containsKey("recovery")){
            values.remove("recovery");
            Log.i("insertingv;", String.valueOf(values));
            String[] x = {keyin};
            String[] arrofst = valuein.split("#",2);
            String valu = arrofst[0];
            String versi =arrofst[1];
            Cursor cursor8 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
            if(cursor8!=null && cursor8.moveToFirst()){
                String forversion= cursor8.getString(cursor8.getColumnIndex("version"));
                if(Integer.parseInt(forversion)> Integer.parseInt(versi)){
                    cursor8.close();
                    return null;
                }
                else {
                    values.remove("value");
                    values.remove("version");
                    values.put("value", valu);
                    values.put("version", String.valueOf(Integer.parseInt(versi)));
                    cursor8.close();
                }

            }
            else {
                values.remove("value");
                values.remove("version");
                values.put("value",valu);
                values.put("version",versi);
                cursor8.close();
            }
            mydb.addrow(values);


        }
        else if(values.containsKey("replica")){
            values.remove("replica");
            String[] x = {keyin};
            Cursor cursor8 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);

            if(cursor8!=null && cursor8.moveToFirst()) {

                String forversion= cursor8.getString(cursor8.getColumnIndex("version"));
                values.remove("version");
                values.put("version", String.valueOf(Integer.parseInt(forversion)+1));
                mydb.addrow(values);
                cursor8.close();
                Cursor cursor9 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
            /*    if(cursor9!=null && !cursor9.moveToFirst()) {
                    Log.i("versioned", cursor9.getString(cursor9.getColumnIndex("key")));
                    Log.i("versioned", cursor9.getString(cursor9.getColumnIndex("value")));
                    Log.i("versioned", cursor9.getString(cursor9.getColumnIndex("version")));
                }
                else{
                    Log.i("hu","nocursoe");
                }*/
            }

            else{
                values.put("version","1");
                mydb.getWritableDatabase().insertWithOnConflict("SimpleDynamo","key",values, SQLiteDatabase.CONFLICT_REPLACE);
                cursor8.close();
            /*    Cursor cursor9 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
                if(cursor9!=null && cursor9.moveToFirst()) {
                    Log.i("versionedbefore", cursor9.getString(cursor9.getColumnIndex("key")));
                    Log.i("versionedbefore", cursor9.getString(cursor9.getColumnIndex("value")));
                    Log.i("versionedbefore", cursor9.getString(cursor9.getColumnIndex("version")));
                }
                else{
                    Log.i("hu","nocursoe");
                }*/
            }





          //  String tablestring = mydb.getTableAsString(mydb,"SimpleDynamo");
          //  Log.i("replicatablestring", tablestring);
           // waitforinsert = false;
            //String tablestring = mydb.getTableAsString(mydb,"SimpleDynamo");
            //Log.i("contentvalues", tablestring);
        }
        else if(values.containsKey("justinsert")){
            values.remove("justinsert");
            Log.i("insertingvalues", String.valueOf(values));
            String[] x = {keyin};
            Cursor cursor8 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
            if(cursor8!=null&& cursor8.moveToFirst()) {
                String forversion= cursor8.getString(cursor8.getColumnIndex("version"));
                values.remove("version");
                values.put("version", String.valueOf(Integer.parseInt(forversion)+1));
                mydb.getWritableDatabase().insertWithOnConflict("SimpleDynamo","key",values, SQLiteDatabase.CONFLICT_REPLACE);
                cursor8.close();
           /*     Cursor cursor9 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
                if(cursor9!=null && cursor9.moveToFirst()) {
                    Log.i("versioned", cursor9.getString(cursor9.getColumnIndex("key")));
                    Log.i("versioned", cursor9.getString(cursor9.getColumnIndex("value")));
                    Log.i("versioned", cursor9.getString(cursor9.getColumnIndex("version")));
                }
                else{
                    Log.i("hu","nocursoe");
                }*/
            }

            else{
                values.put("version","1");
                mydb.getWritableDatabase().insertWithOnConflict("SimpleDynamo","key",values, SQLiteDatabase.CONFLICT_REPLACE);

                cursor8.close();
            /*    Cursor cursor9 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
                if(cursor9!=null && cursor9.moveToFirst()) {
                    Log.i("versionedbefore", cursor9.getString(cursor9.getColumnIndex("key")));
                    Log.i("versionedbefore", cursor9.getString(cursor9.getColumnIndex("value")));
                    Log.i("versionedbefore", cursor9.getString(cursor9.getColumnIndex("version")));
                }
                else{
                    Log.i("hu","nocursoe");
                }*/
            }


         //   String tablestring = mydb.getTableAsString(mydb,"SimpleDynamo");
         //   Log.i("inserttablestring", tablestring);
            // waitforinsert = false;
            //String tablestring = mydb.getTableAsString(mydb,"SimpleDynamo");
            //Log.i("contentvalues", tablestring);
        }
        else {
            try {
                Thread.sleep(50);
                String keyhash = genHash(keyin);
                String myporthash = genHash(String.valueOf(Integer.parseInt(myport) / 2));
                synchronized (this) {
 /////////////////////////////////////////////////////////////finding coordinator/////////////////////////////////////////////////////////////////////////////////
                    dummylist.clear();
                    for (String iter : orderlist) {
                        dummylist.add(iter);
                    }
                    dummylist.add(keyhash);
                    Collections.sort(dummylist);
                    String insertinto;
                    Log.i("dummy", String.valueOf(dummylist));
                    Log.i("dummyordered", String.valueOf(orderlist));
                    if (dummylist.indexOf(keyhash) == dummylist.size() - 1) {
                        insertinto = dummylist.get(0);
                 //       Log.i("insertintolast", insertinto);
                    } else {
                        int index = dummylist.indexOf(keyhash);
                        insertinto = dummylist.get(index + 1);
                 //       Log.i("insertintoelse", insertinto);
                    }
///////////////////////////////////////////////////////////////////////replication and insert if cordinator == mynode///////////////////////////////////////////////////////////
                    if (insertinto.compareTo(myporthash) == 0) {
                  //      Log.i("coordinatorvalues", String.valueOf(values));
                        String[] x = {keyin};
                        Cursor cursor8 = mydb.getReadableDatabase().rawQuery("select * from SimpleDynamo where key" + "= ?", x);
                        if(cursor8==null || !cursor8.moveToFirst()){
                            values.put("version","1");
                        }
                        else {

                            String forversion= cursor8.getString(cursor8.getColumnIndex("version"));
                            values.remove("version");
                            values.put("version", String.valueOf(Integer.parseInt(forversion)+1));
                        }
                        mydb.addrow(values);
                        String replicaports = "";
                        if (orderlist.indexOf(myporthash) >= orderlist.size() - 2) {
                            if (orderlist.indexOf(myporthash) == orderlist.size() - 2) {
                                replicaports = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myporthash) + 1))) + ":" + String.valueOf(ringtable.get(orderlist.get(0)));
                            } else if (orderlist.indexOf(myporthash) == orderlist.size() - 1) {
                                replicaports = String.valueOf(ringtable.get(orderlist.get(0))) + ":" + String.valueOf(ringtable.get(orderlist.get(1)));
                            }
                        } else {
                            replicaports = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myporthash) + 1))) + ":" + String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myporthash) + 2)));
                        }
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replica", myport, replicaports, keyin + ":" + valuein + ":" + String.valueOf(uri));
                        // waitforinsert = false;
                        return uri;
                    }
///////////////////////////////////////////////////////////////////////replication and insert if coordinator!=mynode//////////////////////////////////////////////////////
                    else {
                        String insertport = String.valueOf(ringtable.get(insertinto));
                   //     Log.i("insertport", insertport);
                   //     Log.i("replicafromcoord", String.valueOf(values));
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "insert", myport, insertport, keyin + ":" + valuein + ":" + String.valueOf(uri));
                        String replicaports = "";
                        if (orderlist.indexOf(insertinto) >= orderlist.size() - 2) {
                            if (orderlist.indexOf(insertinto) == orderlist.size() - 2) {
                                replicaports = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(insertinto) + 1))) + ":" + String.valueOf(ringtable.get(orderlist.get(0)));
                            } else if (orderlist.indexOf(insertinto) == orderlist.size() - 1) {
                                replicaports = String.valueOf(ringtable.get(orderlist.get(0))) + ":" + String.valueOf(ringtable.get(orderlist.get(1)));
                            }
                        } else {
                            replicaports = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(insertinto) + 1))) + ":" + String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(insertinto) + 2)));
                        }
                        Log.i("checkingreplica", String.valueOf(replicaports));
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replica", myport, replicaports, keyin + ":" + valuein + ":" + String.valueOf(uri));
                        //  waitforinsert = false;
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("MYPORT", myPort);
        ServerSocket serverSocket = null;
//////////////////////////////////////////////////////////////////////////start server/////////////////////////////////////////////////////////////////////////
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "onCreate: socketcreationexception");
            e.printStackTrace();
        }

     /*   try {
            Thread.sleep(40);
        } catch (InterruptedException e) {
            Log.e(TAG, "onCreate: waitexception");
            e.printStackTrace();
        }*/
        //stackoverflow bit.
        //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"gossip",myPort);
//////////////////////////////////////////////////////////////////////////////check of restart from failure///////////////////////////////////////////////////
     /*   Boolean rowExists;
        File dbFile = getContext().getDatabasePath("SimpleDynamo");
        boolean checkFlag = true;
        sqlitedb testDb;
        File file = new File (String.valueOf(dbFile));
        if(file.exists()){
            Log.e("restarttrue", "true");
            restart = true;
        }*/

//////////////////////////////////////////////////////////////////////////////initializing orderlist and ringtable//////////////////////////////////////////////////
        try {
            final String myhash = genHash(String.valueOf(Integer.parseInt(myPort) / 2));
            for(String port:remoteports){
                Log.e("port", genHash(String.valueOf(Integer.parseInt(port)/2))+":"+port);
                if(!orderlist.contains(genHash(String.valueOf(Integer.parseInt(port)/2)))) {
                    orderlist.add(genHash(String.valueOf(Integer.parseInt(port) / 2)));
                    replicalist.add(genHash(String.valueOf(Integer.parseInt(port) / 2)));
                    ringtable.put(genHash(String.valueOf(Integer.parseInt(port) / 2)), port);
                }
            }
            Collections.sort(orderlist);
            Collections.sort(replicalist);


            Log.e("allport", String.valueOf(orderlist)+":"+String.valueOf(ringtable));
////////////////////////////////////////////////////////////////////////recovery thread//////////////////////////////////////////////////////////////////////////
            if(restart) {
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "recovery", myPort, myhash);

                Log.e("alhash", myhash);
            }
            try {
                Thread.sleep(60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //ringtable.put(myPort, myhash);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "onCreate: genhashexception");
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        sqlitedb mydb1 = new sqlitedb(getContext());
        ArrayList<String> dummylist = new ArrayList<String>();
        String keyqu = selection;
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myport = String.valueOf((Integer.parseInt(portStr))*2);

     /*   while(waitforquery){
            Log.v("waitingforquery",selection);
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        waitforquery = true;*/
     /*   if (firstquery) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            firstquery = false;
        }*/
        Log.i("startqueryvalues", selection);

       /* if(sortOrder!=null &&sortOrder.compareTo("Queryforward")==0){
            String[] arrofstr = selection.split(":",2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"query*return",myport,arrofstr[1],arrofstr[0]);
           // waitforquery = false;
            return null;
        }*/
///////////////////////////////////////////////////////////////////////////selection of local data///////////////////////////////////////////////////////////////
        if(selection.equals("@")) {
            String s = mydb1.getTableAsString(mydb1, "SimpleDynamo");
            Log.i("tablestringattherate", s);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                    e.printStackTrace();
            }
            Cursor cursor2 = mydb1.getReadableDatabase().rawQuery("select key, value from SimpleDynamo", null);
           // waitforquery = false;
            return cursor2;
        }
//////////////////////////////////////////////////////////////////////////////selection of globall data////////////////////////////////////////////////////////
        else if(selection.equals("*")){
           // waitforquery = false;
            //sqlitedb mydb1 = new sqlitedb(getContext());
            Log.v("query", selection);
            Log.v("cursor", "enteredquerystar");
            String[] cn = {
                    "key", "value"
            };
            MatrixCursor cursor = new MatrixCursor(cn);
            Log.v("remportssize", String.valueOf(orderlist.size()));
            for (int i = 0; i < orderlist.size(); i++) {
                String queryportall = String.valueOf(ringtable.get(orderlist.get(i)));
                try {
                    String allvalue = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query*", myport, queryportall).get();

          /*      while (querynow == false) {
                    Log.v("waitinghash", String.valueOf(queryvalues));
                    try {
                        Thread.sleep(40);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //  break;

                }*/
                    //   querynow = false;
                    if(allvalue!=null) {
                        String value = allvalue;
                        ContentValues cv;
                        Log.i("valueofup", String.valueOf(value));
                        value = value.substring(1, value.length() - 1);//remove curly brackets
                        String[] keyValuePairs = value.split(",");
                        //split the string to creat key-value pairs
                        for (String pair : keyValuePairs)                        //iterate over the pairs
                        {
                            String[] entry = pair.split("=");                   //split the pairs to get key and value
                            String ke = entry[0].trim();
                            String va = entry[1].trim();
                            String[] row = {
                                    ke, va
                            };
                            cursor.addRow(row);
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Log.i("valueofup","neglect");
                    continue;

                } catch (ExecutionException e) {
                    Log.i("valueofup","neglect");
                    e.printStackTrace();
                    continue;
                }
            }
           // waitforquery = false;
            return cursor;

        }
/////////////////////////////////////////////////////////////////////selection of specific node//////////////////////////////////////////////////////////////////////////
        else{
            try {
              //  String s = mydb1.getTableAsString(mydb1, "SimpleDynamo");
              //  Log.i("tablestring", s);
                Thread.sleep(50);
                String keyhash = genHash(keyqu);
                String myporthash = genHash(String.valueOf(Integer.parseInt(myport) / 2));
            /*    while(waitforsortquery){
                    Log.v("waitforinsertininsert",selection);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                waitforsortquery = true;*/
           // synchronized (this) {
//////////////////////////////////////////////////////////////////////finding coordinator/////////////////////////////////////////////////////////////////
                dummylist.clear();
                for (String iter : orderlist) {
                    dummylist.add(iter);
                }
                dummylist.add(keyhash);
                Collections.sort(dummylist);
                String queryinto;
                Log.i("dummyquery", String.valueOf(dummylist));
                Log.i("dummyorderedquery", String.valueOf(orderlist));
                if (dummylist.indexOf(keyhash) == dummylist.size() - 1) {
                    queryinto = dummylist.get(0);
              //      Log.i("queryintolast", queryinto);
                } else {
                    int index = dummylist.indexOf(keyhash);
                    queryinto = dummylist.get(index + 1);
             //       Log.i("queryintoelse", queryinto);
                }
                //    waitforsortquery = false;
                String tablestring = mydb1.getTableAsString(mydb1,"SimpleDynamo");

                String qports;
                qports= String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(queryinto))));
                if (orderlist.indexOf(queryinto) >= orderlist.size() - 2) {
                    if (orderlist.indexOf(queryinto) == orderlist.size() - 2) {
                      qports = qports+":"+String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(queryinto) + 1)))+":"+String.valueOf(ringtable.get(orderlist.get(0)));

                    } else if (orderlist.indexOf(queryinto) == orderlist.size() - 1) {
                        qports = qports+":"+String.valueOf(ringtable.get(orderlist.get(0)))+":"+String.valueOf(ringtable.get(orderlist.get(1)));
                    }
                } else {
                    qports = qports+":"+String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(queryinto) + 1)))+":"+String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(queryinto) + 2)));
                }
                String returnval = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query", myport, qports, keyqu + ":" + String.valueOf(uri)).get();
                String[] cn = {
                        "key", "value"
                };
                String[] arrofstr = returnval.split(":",2);
                MatrixCursor cursor = new MatrixCursor(cn);
                String[] row = {
                        arrofstr[0], arrofstr[1]
                };
                cursor.addRow(row);
            //    Log.i("tablestring", tablestring);
                Log.v("returnval", returnval);
                return cursor;
 ////////////////////////////////////////////////////////////////////////////////if coordinator == mynode//////////////////////////////////////////////////////
           /*     if (queryinto.compareTo(myporthash) == 0) {
                 //   String s = mydb1.getTableAsString(mydb1, "SimpleDynamo");
                 //     Log.i("tablestring", s);
                    Log.i("querycoordinatorvalues", String.valueOf(keyqu));
                    String returnport;
                    String[] x = {selection};
                    Cursor cursor4 = mydb1.getReadableDatabase().rawQuery("select key,value from SimpleDynamo where key" + "= ?", x);
                    if (cursor4 != null && cursor4.moveToFirst()) {
                        Log.v("querycursor", String.valueOf(cursor4));
                        String valuequ = cursor4.getString(cursor4.getColumnIndex("value"));
                        Log.v("querycursorvalue", valuequ);
                        if (sortOrder == null) {
                            //          waitforquery=false;
                            return cursor4;
                            //returnport = myport;
                        }*/
                       /* else {
                            returnport = sortOrder;
                        }
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "returnqueryvalue", myport, returnport, keyqu + ":" + valuequ + ":" + String.valueOf(uri));
                        cursor4.close();
                        if (sortOrder != null) {
                            //         waitforquery=false;
                            return cursor4;
                        }-*/
                  //  }
                   /* else {
                        return cursor4;
                    }*/
             //   }
/////////////////////////////////////////////////////////////////////////if coordinator is not my node///////////////////////////////////////////////////////////
          /*      else {
                    String queryport = String.valueOf(ringtable.get(queryinto));
                    Log.i("queryport", queryport);
                    String returnval = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query", myport, queryport, keyqu + ":" + String.valueOf(uri)).get();
                    String[] cn = {
                            "key", "value"
                    };
                    String[] arrofstr = returnval.split(":",2);
                    MatrixCursor cursor = new MatrixCursor(cn);
                    String[] row = {
                            arrofstr[0], arrofstr[1]
                    };
                    cursor.addRow(row);
                    Log.v("returnval", returnval);
                    return cursor;
                }*/


                //iswaiting = true;
       /*        while (querynow == false && queryvalues != null) {
                    Log.v("waiting", String.valueOf(queryvalues));
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //  break;
                }
                //iswaiting = false;
                querynow = false;
                Log.v("aenteredquerynow", String.valueOf(queryvalues));
                String[] cn = {
                        "key", "value"
                };
                MatrixCursor cursor = new MatrixCursor(cn);
                String[] row = {
                        queryvalues[0], queryvalues[1]
                };
                cursor.addRow(row);
                Arrays.fill(queryvalues, null);
                // waitforquery = false;
                //      waitforquery=false;

                return cursor;*/
         //   }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myport = String.valueOf((Integer.parseInt(portStr) * 2));
        if(selection==null) {
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "gossip", myport);
            Log.v("recovernode", "gossip");
        }

        else {
            String[] arrofstr = selection.split(":", 2);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query*return", myport, arrofstr[1], arrofstr[0]);
        }
            // waitforquery = false;
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            synchronized (this) {
                ServerSocket serverSocket = sockets[0];
                Log.e(TAG, "connection established");
                Object object = new Object();
                try {
                    serverSocket.setSoTimeout(1000000000);
                    while (true) {
                        try {
                            Log.e(TAG, "before accept");
                            Socket socket = serverSocket.accept();
                            Log.e(TAG, "after accept");
                            ArrayList<String> serverarr = null;
                            serverarr = new ArrayList<String>();
                            ObjectInputStream objectInput = new ObjectInputStream(socket.getInputStream()); //Error Line!
                            object = objectInput.readObject();
                            serverarr = (ArrayList<String>) object;
                            // objectInput.close();
                            Log.i("recvd 1st msg frm clint", String.valueOf(serverarr));
                            String purpose = serverarr.get(0);
                            if (purpose.compareTo("gossip") == 0) {
                                String port = serverarr.get(1);
                                String porthash = genHash(String.valueOf(Integer.parseInt(port) / 2));
                                ringtable.put(porthash, port);
                                if (!orderlist.contains(porthash)) {
                                    orderlist.add(porthash);
                                }
                                Collections.sort(orderlist);
                                ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                objectOutput.writeObject(String.valueOf(ringtable) + ":" + String.valueOf(orderlist));
                                Log.i("updated ringtable", String.valueOf(ringtable));
                                Log.i("updated orderlist", String.valueOf(orderlist));
                            } else if (purpose.compareTo("insert") == 0) {
                                String[] arrofstr = serverarr.get(1).split(":", 3);
                                ContentResolver cr = getContext().getContentResolver();
                                ContentValues cv;
                                cv = new ContentValues();
                                cv.put("key", arrofstr[0]);
                                cv.put("value", arrofstr[1]);
                                cv.put("justinsert", "true");
                                Uri u = Uri.parse(arrofstr[2]);
                                Log.i("uri", String.valueOf(u));
                                cr.insert(mUri, cv);
                            } else if (purpose.compareTo("replica") == 0) {

                                String[] arrofstr = serverarr.get(1).split(":", 3);
                                Log.i("replicadoing",arrofstr[0]+":"+arrofstr[1]);
                                ContentResolver cr = getContext().getContentResolver();
                                ContentValues cv;
                                cv = new ContentValues();
                                cv.put("key", arrofstr[0]);
                                cv.put("value", arrofstr[1]);
                                cv.put("replica", "true");
                                Uri u = Uri.parse(arrofstr[2]);
                                Log.i("uri", String.valueOf(u));
                                cr.insert(mUri, cv);
                            } else if (purpose.compareTo("returnqueryvalue") == 0) {
                                synchronized (this) {
                                    String[] arrofstr = serverarr.get(1).split(":", 3);
                                    String keyqu = arrofstr[0];
                                    String valuequ = arrofstr[1];
                                    Uri u = Uri.parse(arrofstr[2]);
                                    String[] columnNames = {
                                            "key", "value"
                                    };
                                    MatrixCursor cursor = new MatrixCursor(columnNames);
                                    String[] row = {
                                            keyqu, valuequ
                                    };
                                    cursor.addRow(row);
                                    queryvalues = row;
                                    Log.i("queryvaluesresult", keyqu + ":" + valuequ);
                              /*  while (iswaiting = false) {
                                    Log.i("iswaiting", "iswait");
                                    try {
                                        Thread.sleep(400);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }*/
                                    querynow = true;
                                }
                            } else if (purpose.compareTo("query") == 0) {
                                synchronized (this) {
                                    String[] arrofstr = serverarr.get(1).split(":", 2);
                                    String keyqu = arrofstr[0];
                                    Uri u = Uri.parse(arrofstr[1]);
                                    Log.i("cuesorquery", serverarr.get(1));
                                    sqlitedb mydb1 = new sqlitedb(getContext());
                                    String[] x = {keyqu};
                                    Cursor cursorr = mydb1.getReadableDatabase().rawQuery("select key,value,version from SimpleDynamo where key" + "= ?", x);
                                    Log.v("querycursorserver", String.valueOf(cursorr));
                                    if (cursorr != null && cursorr.moveToFirst()) {
                                        String valuequ = cursorr.getString(cursorr.getColumnIndex("value"));
                                        String version = cursorr.getString(cursorr.getColumnIndex("version"));
                                        Log.v("querycursorserver", keyqu + ":" + valuequ);
                                        ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                        objectOutput.writeObject(keyqu + ":" + valuequ + ":" + version);
                                        // getContext().getContentResolver().update(mUri,null,keyqu+":"+valuequ+":"+serverarr.get(2), new String[]{"querypass"});
                                    } else {
                                        Log.v("nokeynull", "null");
                                        // Cursor cursorr1 = query(mUri,null,keyqu,null,null);

                                        ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                        objectOutput.writeObject("nokey");
                                    }
                                }
                            } else if (purpose.compareTo("query*") == 0) {
                                Cursor cursor4 = getContext().getContentResolver().query(mUri, null, "@", null, "Querynow");
                                Hashtable<String, String> hash_tableserv =
                                        new Hashtable<String, String>();
                                String queriedport = serverarr.get(1);
                                //       Log.i("beforecurserstar", String.valueOf(hash_tableserv));
                                if (cursor4.moveToFirst()) {
                                    do {
                                        //             Log.i("do", String.valueOf(hash_tableserv));
                                        String ke = cursor4.getString(cursor4.getColumnIndex("key"));
                                        String valu = cursor4.getString(cursor4.getColumnIndex("value"));
                                        //             Log.i("dolat", String.valueOf(hash_tableserv));
                                        String[] rowst = {
                                                ke, valu
                                        };

                                        hash_tableserv.put(ke, valu);
                                        Log.i("whiledo", String.valueOf(hash_tableserv));
                                        // do what you need with the cursor here
                                    } while (cursor4.moveToNext());
                                    String ht = hash_tableserv.toString();
                                    //          Log.i("aftersentcurserstar", String.valueOf(hash_tableserv));
                                    ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                    objectOutput.writeObject(ht);
                                    // getContext().getContentResolver().update(mUri,null,ht+":"+queriedport,null);
                                } else {
                                    String ht = "nokey";
                                    //         Log.i("nocurdata", "null");
                                    ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                    objectOutput.writeObject(ht);
                                }
                                cursor4.close();
                            }
                 /*       else if(purpose.compareTo("query*return")==0) {
                            String value1 = serverarr.get(1);doi
                            if (value1.compareTo("null") != 0) {
                                String value = serverarr.get(1);
                                value = value.substring(1, value.length() - 1);           //remove curly brackets
                                String[] keyValuePairs = value.split(",");              //split the string to creat key-value pairs

                                for (String pair : keyValuePairs)                        //iterate over the pairs
                                {ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                    objectOutput.writeObject(keyqu+":"+valuequ);
                                    String[] entry = pair.split("=");                   //split the pairs to get key and value
                                    hash_table.put(entry[0].trim(), entry[1].trim());          //add them to the hashmap and trim whitespaces
                                }
                                Log.i("queryallsend", String.valueOf(hash_table));
                                querynow = true;
                            }
                            else{
                                querynow=true;
                            }
                        }*/
                            else if (purpose.compareTo("recovery") == 0) {
                                synchronized (this) {
                                    sqlitedb mydb1 = new sqlitedb(getContext());
                                    String s = mydb1.getTableAsString(mydb1, "SimpleDynamo");
                                    Cursor cursor4 = mydb1.getReadableDatabase().rawQuery("select key,value,version from SimpleDynamo", null);
                                    Hashtable<String, String> hash_tableserv =
                                            new Hashtable<String, String>();
                                    //     String queriedport = serverarr.get(1);
                                    //       Log.i("beforecurserstar", String.valueOf(hash_tableserv));
                                    String ht = "";
                                    if (cursor4.moveToFirst()) {
                                        do {
                                            //          Log.i("do", String.valueOf(hash_tableserv));
                                            String ke = cursor4.getString(cursor4.getColumnIndex("key"));
                                            String valu = cursor4.getString(cursor4.getColumnIndex("value"));
                                            String versi = cursor4.getString(cursor4.getColumnIndex("version"));
                                            //     Log.i("dolat", String.valueOf(hash_tableserv));
                                            String[] rowst = {
                                                    ke, valu + "#" + versi
                                            };

                                            hash_tableserv.put(ke, valu + "#" + versi);
                                            //           Log.i("whiledo", String.valueOf(hash_tableserv));
                                            // do what you need with the cursor here
                                        } while (cursor4.moveToNext());

                                        //        Log.i("sizerec", String.valueOf(recoverytable.size()));
                             /*   if (recoverytable.size() == 0) {
                                    ht = "null";
                                } else {
                                    ht = recoverytable.toString();
                                }*/
                                        ht = String.valueOf(hash_tableserv);
                                        //         Log.i("aftersentcurserstar", String.valueOf(ht));
                                        ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                        objectOutput.writeObject(ht);

                                        //recoverytable.clear();
                                    } else {
                                        ht = "null";
                                        ObjectOutputStream objectOutput = new ObjectOutputStream(socket.getOutputStream());
                                        objectOutput.writeObject(ht);
                                    }

                                }
                            } else if (purpose.compareTo("recoveryupdate") == 0) {
                                synchronized (this) {
                                    String myhash = serverarr.get(2);
                                    Collections.sort(orderlist);
                                    String[] succsor = new String[2];
                                    if (orderlist.indexOf(myhash) >= orderlist.size() - 2) {
                                        if (orderlist.indexOf(myhash) == orderlist.size() - 2) {
                                            succsor[0] = (String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myhash) + 1))));
                                            succsor[1] = (String.valueOf(ringtable.get(orderlist.get(0))));
                                        } else if (orderlist.indexOf(myhash) == orderlist.size() - 1) {
                                            succsor[0] = (String.valueOf(ringtable.get(orderlist.get(0))));
                                            succsor[1] = (String.valueOf(ringtable.get(orderlist.get(1))));
                                        }
                                    } else {
                                        succsor[0] = (String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myhash) + 1))));
                                        succsor[1] = (String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myhash) + 2))));
                                    }
                                    //      Log.e("succsor", succsor[0]+":"+succsor[1]);
                                    String[] arrofstr = serverarr.get(1).split(":", 5);
                                    //  String value1 = serverarr.get(1);
                                    ContentValues cv;
                                    ContentResolver cr = getContext().getContentResolver();
                                    for (int i = 0; i < arrofstr.length; i++) {

                                        if (arrofstr[i].compareTo("") != 0) {
                                            String value = arrofstr[i];
                                            //         Log.i("valueofup", String.valueOf(value));
                                            value = value.substring(1, value.length() - 1);           //remove curly brackets
                                            String[] keyValuePairs = value.split(",");
                                            //split the string to creat key-value pairs
                                            for (String pair : keyValuePairs)                        //iterate over the pairs
                                            {
                                                String[] entry = pair.split("=");                   //split the pairs to get key and value
                                                cv = new ContentValues();
                                                String ke = entry[0].trim();
                                                String val = entry[1].trim();
                                                String va = val.split("#", 2)[0];
                                                String versio = val.split("#", 2)[1];
                                                String kehash = genHash(ke);
                                                String suc1 = "";
                                                String suc2 = "";
                                                //     cv.put("key", ke);
                                                //     cv.put("value", va);

                                                replicalist.add(kehash);
                                                Collections.sort(replicalist);
                                                //       Log.i("replicalistss", String.valueOf(replicalist));
                                                //       Log.i("recoveringdatalast0", String.valueOf(cv));
                                                if (replicalist.indexOf(kehash) == replicalist.size() - 1) {
                                                    suc1 = String.valueOf(ringtable.get(replicalist.get(0)));
                                                    suc2 = String.valueOf(ringtable.get(replicalist.get(1)));
                                                    //          Log.i("succ1+succ2", suc1+":"+suc2);
                                                    //  if(succsor[0].compareTo(suc1)==0||succsor[0].compareTo(suc2)==0||succsor[1].compareTo(suc1)==0||succsor[1].compareTo(suc2)==0){
                                                    if (succsor[0].compareTo(suc1) == 0 || succsor[1].compareTo(suc1) == 0) {
                                                        //            Log.i("containssucc", "yes");
                                                        replicalist.remove(kehash);
                                                        continue;
                                                    } else {
                                                        cv.put("key", ke);
                                                        cv.put("value", val);
                                                        cv.put("recovery", "true");
                                                        //     Log.i("recoveringdatalast1", String.valueOf(cv));
                                                        //          Log.i("recoveringdatalast1con", String.valueOf(cv));
                                                        replicalist.remove(kehash);
                                                        cr.insert(mUri, cv);

                                                    }

                                                } else if (replicalist.indexOf(kehash) == replicalist.size() - 2) {
                                                    suc1 = String.valueOf(ringtable.get(replicalist.get(replicalist.size() - 1)));
                                                    suc2 = String.valueOf(ringtable.get(replicalist.get(0)));
                                                    Log.i("succ1+succ2", suc1 + ":" + suc2);
                                                    Log.i("succ1+succ2test", String.valueOf(succsor[0].compareTo(suc1) == 0 || succsor[0].compareTo(suc2) == 0 || succsor[1].compareTo(suc1) == 0 || succsor[1].compareTo(suc2) == 0));
                                                    //   if(succsor[0].compareTo(suc1)==0||succsor[0].compareTo(suc2)==0||succsor[1].compareTo(suc1)==0||succsor[1].compareTo(suc2)==0){
                                                    if (succsor[0].compareTo(suc1) == 0 || succsor[1].compareTo(suc1) == 0) {
                                                        //   Log.i("recoveringdatalast2con", String.valueOf(cv));
                                                        //          Log.i("containssucc", "yes");
                                                        replicalist.remove(kehash);
                                                        continue;
                                                    } else {
                                                        cv.put("key", ke);
                                                        cv.put("value", val);
                                                        cv.put("recovery", "true");
                                                        //         Log.i("recoveringdatalast2", String.valueOf(cv));
                                                        replicalist.remove(kehash);
                                                        cr.insert(mUri, cv);

                                                    }
                                                } else {
                                                    suc1 = String.valueOf(ringtable.get(replicalist.get(replicalist.indexOf(kehash) + 1)));
                                                    suc2 = String.valueOf(ringtable.get(replicalist.get(replicalist.indexOf(kehash) + 2)));
                                                    Log.i("succ1+succ2", suc1 + ":" + suc2);
                                                    //        if(succsor[0].compareTo(suc1)==0||succsor[0].compareTo(suc2)==0||succsor[1].compareTo(suc1)==0||succsor[1].compareTo(suc2)==0){
                                                    if (succsor[0].compareTo(suc1) == 0 || succsor[1].compareTo(suc1) == 0) {
                                                        Log.i("containssucc", "yes");
                                                        replicalist.remove(kehash);
                                                        continue;
                                                    } else {
                                                        cv.put("key", ke);
                                                        cv.put("value", val);
                                                        cv.put("recovery", "true");
                                                        //      Log.i("recoveringdatalast3", String.valueOf(cv));
                                                        replicalist.remove(kehash);
                                                        cr.insert(mUri, cv);
                                                    }
                                                }

                                                //add them to the hashmap and trim whitespaces
                                            }
                                        }
                                    }
                                }

                            } else if (purpose.compareTo("deleteal") == 0) {
                                sqlitedb mydb1 = new sqlitedb(getContext());
                                mydb1.getReadableDatabase().execSQL("delete from " + "SimpleDynamo");
                            }

                        } catch (IOException e) {
                            Log.e(TAG, "doInBackground: socket accept exception");
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "doInBackground: readobjectexception");
                            e.printStackTrace();
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }

                    }
                } catch (SocketException e) {
                    Log.e(TAG, "doInBackground: sockettimeoutexception");
                    e.printStackTrace();
                }
           /* try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }*/
                return null;
            }
        }
    }


}
