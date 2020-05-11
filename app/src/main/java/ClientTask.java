
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.Hashtable;

import static android.content.ContentValues.TAG;

public class ClientTask extends AsyncTask<String, Void, String> {
    String[] remoteports= new String[]{"11108","11112","11116","11120","11124"};
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    @Override
    protected String doInBackground(String... msgs) {
        ArrayList<String> myarrlistg = new ArrayList<String>();
        ArrayList<String> myarrlisti = new ArrayList<String>();
        ArrayList<String> myarrlistr = new ArrayList<String>();
        ArrayList<String> myarrlistq = new ArrayList<String>();
        ArrayList<String> myarrlistqret = new ArrayList<String>();
        ArrayList<String> myarrlistrec = new ArrayList<String>();
        ArrayList<String> myarrlistreco = new ArrayList<String>();
        String myPort = msgs[1];
        String purpose = msgs[0];
        if (purpose.compareTo("gossip")==0){
            myarrlistg.add(msgs[0]);
            myarrlistg.add(msgs[1]);
            Socket[] socketobj = new Socket[5];
            ObjectOutputStream[] objOutput = new ObjectOutputStream[5];
            ObjectInputStream[] objInput = new ObjectInputStream[5];
            try {
                for (int i = 0; i < remoteports.length; i++) {
                    String remotePort = remoteports[i];
                    socketobj[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    try {
                        objOutput[i] = new ObjectOutputStream(socketobj[i].getOutputStream());
                        objOutput[i].writeObject(myarrlistg);
                        objInput[i] = new ObjectInputStream(socketobj[i].getInputStream());
                        Object object1 = objInput[i].readObject();
                        String returnhash = (String) object1;
                        if(returnhash!=null) {
                            String[] arrofstr = returnhash.split(":", 2);
                            ArrayList<String> myList = new ArrayList<String>();
                            String remportsstr = arrofstr[1].substring(1, arrofstr[1].length() - 1);
                            String[] portpai = remportsstr.split(",");
                            Log.e("Updatedorderclientside", String.valueOf(returnhash));
                            if (portpai.length > orderlist.size()) {
                                orderlist.clear();
                                for (String pai : portpai)                        //iterate over the pairs
                                {
                                    orderlist.add(pai.trim());
                                    //add them to the arraylist and trim whitespaces
                                }
                                String value = arrofstr[0];
                                value = value.substring(1, value.length() - 1);           //remove curly brackets
                                String[] keyValuePairs = value.split(",");              //split the string to creat key-value pairs

                                for (String pair : keyValuePairs)                        //iterate over the pairs
                                {
                                    String[] entry = pair.split("=");                   //split the pairs to get key and value
                                    ringtable.put(entry[0].trim(), entry[1].trim());          //add them to the hashmap and trim whitespaces
                                }
                            }
                        }


                    } catch (IOException e) {
                        Log.e(TAG, "doInBackgroundgossip: objectoutputstreamexec");
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if(purpose.compareTo("insert")==0){
            String insertinto = msgs[2];
            myarrlisti.add(msgs[0]);
            myarrlisti.add(msgs[3]);
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(insertinto));
                ObjectOutputStream objOutput = new ObjectOutputStream(socket.getOutputStream());
                objOutput.writeObject(myarrlisti);
            }catch (IOException e) {
                Log.e(TAG, "doInBackgroundinsert: objectoutputstreamexec");
                String[] arrofstr = msgs[3].split(":",3);
                recoverytable.put(arrofstr[0],arrofstr[1]);
             //   Log.e("recoverytable", String.valueOf(recoverytable)+"::::::"+insertinto);
                e.printStackTrace();
            }
        }
        else if(purpose.compareTo("replica")==0){
            synchronized (this) {
                String str = msgs[2];
                myarrlistr.add(msgs[0]);
                myarrlistr.add(msgs[3]);
                String[] arrofstr = str.split(":", 2);
                Socket[] socketobj = new Socket[2];
                ObjectOutputStream[] objOutput1 = new ObjectOutputStream[2];
                ObjectInputStream[] objInput = new ObjectInputStream[2];
                try {
                    for (int i = 0; i < arrofstr.length; i++) {
                        String remotePort = arrofstr[i];
                        socketobj[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        try {
                            objOutput1[i] = new ObjectOutputStream(socketobj[i].getOutputStream());
                            objOutput1[i].writeObject(myarrlistr);
                            Log.e(TAG, "replicano."+remotePort);
                        } catch (IOException e) {
                            Log.e(TAG, "doInBackgroundreplica: objectoutputstreamexec");
                           // String[] arrofst = msgs[3].split(":", 3);
                           // recoverytable.put(arrofst[0], arrofst[1]);
                           // Log.e("replicarecoverytable", String.valueOf(recoverytable)+"::::::"+remotePort);
                            e.printStackTrace();
                        }
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        else if(purpose.compareTo("query")==0){
            synchronized (this) {
                //Socket[] socketobj = new Socket[5];
                //ObjectOutputStream[] objOutput = new ObjectOutputStream[5];
                //ObjectInputStream[] objInput = new ObjectInputStream[5];
                String[] arrofstr = msgs[2].split(":", 3);
                Log.e("queryports", msgs[2]);

                myarrlistq.add(msgs[0]);
                myarrlistq.add(msgs[3]);
                myarrlistq.add(msgs[1]);
                Socket[] socketobj = new Socket[3];
                ObjectOutputStream[] objOutput = new ObjectOutputStream[3];
                ObjectInputStream[] objInput = new ObjectInputStream[3];
                //   Hashtable<String,String> keyval = new Hashtable<String,String>();
                   ArrayList<String> kva = new ArrayList<String>();
                int maxversion = 0;
                String keyval = null;
                for (int i = 0; i < arrofstr.length; i++) {
                    try {
                        socketobj[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(arrofstr[i]));
                        objOutput[i] = new ObjectOutputStream(socketobj[i].getOutputStream());
                        objOutput[i].writeObject(myarrlistq);
                        ObjectInputStream objectInput1 = new ObjectInputStream(socketobj[i].getInputStream());
                        Object object1 = objectInput1.readObject();
                        String returnval = (String) object1;
                        if(returnval.compareTo("nokey")==0){
                            continue;
                        }
                        kva.add(returnval);
                        Log.e("kvabefore", String.valueOf(kva));
                        String[] arrofstrvers = returnval.split(":", 3);
                        if (Integer.parseInt(arrofstrvers[2]) > maxversion) {
                            maxversion = Integer.parseInt(arrofstrvers[2]);
                            keyval = arrofstrvers[0] + ":" + arrofstrvers[1];
                        }
                        // keyval.put(arrofstr[2],arrofstr[0]+":"+arrofstr[1]);

                    } catch (IOException e) {
                        Log.e(TAG, "doInBackgroundquery: objectoutputstreamexec");
                        e.printStackTrace();
                        continue;

                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        continue;
                    }
                }
                Log.e("kva", String.valueOf(kva));
                return keyval;
               /* String replicaque;
                try {
                    String queryintohash = genHash(String.valueOf(Integer.parseInt(queryinto)/2));

                if(orderlist.indexOf(queryintohash)==orderlist.size()-1) {
                    replicaque = String.valueOf(ringtable.get(orderlist.get(0)));
                }
                else{
                    replicaque = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(queryintohash)+1)));
                }

                    Log.e("replicaque", replicaque);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(replicaque));
                        ObjectOutputStream objOutput = new ObjectOutputStream(socket.getOutputStream());
                        objOutput.writeObject(myarrlistq);
                        ObjectInputStream objectInput1 = new ObjectInputStream(socket.getInputStream());
                        Object object1 = objectInput1.readObject();
                        String returnval = (String) object1;
                        return returnval;

                }catch (NoSuchAlgorithmException e1) {
                    e1.printStackTrace();
                }catch (IOException e1) {
                        e1.printStackTrace();
                    } catch (ClassNotFoundException e1) {
                        e1.printStackTrace();
                    }

                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }*/
            }
        }
        else if(purpose.compareTo("returnqueryvalue")==0){
            String queryreturninto = msgs[2];
            myarrlistqret.add(msgs[0]);
            myarrlistqret.add(msgs[3]);
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(queryreturninto));
                ObjectOutputStream objOutput = new ObjectOutputStream(socket.getOutputStream());
                objOutput.writeObject(myarrlistqret);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if(purpose.compareTo("query*")==0){
            String queryinto = msgs[2];
            myarrlistq.add(msgs[0]);
            myarrlistq.add(msgs[1]);
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(queryinto));
                ObjectOutputStream objOutput = new ObjectOutputStream(socket.getOutputStream());
                objOutput.writeObject(myarrlistq);
                ObjectInputStream objectInput1 = new ObjectInputStream(socket.getInputStream());
                Object object1 = objectInput1.readObject();
                String returnval = (String) object1;
                return returnval;
            }catch (IOException e) {
                Log.e("querystrfail","fail");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else if(purpose.compareTo("query*return")==0){
            String queryinto = msgs[2];
            myarrlistq.add(msgs[0]);
            myarrlistq.add(msgs[3]);
            myarrlistq.add(msgs[1]);
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(queryinto));
                ObjectOutputStream objOutput = new ObjectOutputStream(socket.getOutputStream());
                objOutput.writeObject(myarrlistq);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if(purpose.compareTo("recovery")==0){
            synchronized (this) {
                String myport = msgs[1];
                String myhash = msgs[2];
                String[] replicaports = new String[2];
                ArrayList overallrec = new ArrayList();
                myarrlistrec.add(msgs[0]);
                // myarrlistg.add(msgs[1]);
                Log.e("replication", String.valueOf(orderlist));
                Log.e("replicationmyhash", String.valueOf(myhash));
                Collections.sort(orderlist);
                if (orderlist.indexOf(myhash) == orderlist.size() - 1) {
                    replicaports[0] = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myhash) - 1)));
                    replicaports[1] = String.valueOf(ringtable.get(orderlist.get(0)));

                }
                else if(orderlist.indexOf(myhash) == 0){
                    replicaports[0] = String.valueOf(ringtable.get(orderlist.get(orderlist.size()-1)));
                    replicaports[1] = String.valueOf(ringtable.get(orderlist.get(1)));

                }
                else {
                    replicaports[0] = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myhash) - 1)));
                    replicaports[1] = String.valueOf(ringtable.get(orderlist.get(orderlist.indexOf(myhash) + 1)));
                }
            //    Log.e("replicationportsarray", replicaports[0]+":"+replicaports[1]);
                Socket[] socketobj = new Socket[2];
                ObjectOutputStream[] objOutput = new ObjectOutputStream[2];
                ObjectInputStream[] objInput = new ObjectInputStream[2];

                try {
                    for (int i = 0; i < replicaports.length; i++) {
                        String remotePort = replicaports[i];
                        Log.e("replication", remotePort);
                        socketobj[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        try {
                            objOutput[i] = new ObjectOutputStream(socketobj[i].getOutputStream());
                            objOutput[i].writeObject(myarrlistrec);
                            objInput[i] = new ObjectInputStream(socketobj[i].getInputStream());
                            Object object1 = objInput[i].readObject();
                            String returnhash = (String) object1;
                      //      Log.e("replicationrec", returnhash);
                            if (returnhash.compareTo("null") != 0) {
                                overallrec.add(returnhash);
                       //         Log.e("retha", returnhash);
                            } else {
                                continue;
                            }

                        } catch (IOException e) {
                            Log.e(TAG, "doInBackgroundgossip: objectoutputstreamexec");
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                    if (overallrec.size() != 0) {
                //        Log.e("returedall", String.valueOf(overallrec));
                        String allrec = "";
                        for (Object itr : overallrec) {
                            allrec = itr + ":" + allrec;

                        }
                        Log.e("allrec", allrec);
                        myarrlistreco.add("recoveryupdate");
                        myarrlistreco.add(allrec);
                        myarrlistreco.add(myhash);
                        Socket sockets = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(myPort));
                        ObjectOutputStream obj = new ObjectOutputStream(sockets.getOutputStream());
                        obj.writeObject(myarrlistreco);
                    }


                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }
        else if(purpose.compareTo("deleteal")==0){
            String queryinto = msgs[2];
            myarrlistq.add(msgs[0]);
            myarrlistq.add(msgs[1]);
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(queryinto));
                ObjectOutputStream objOutput = new ObjectOutputStream(socket.getOutputStream());
                objOutput.writeObject(myarrlistq);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return null;
    }
}
