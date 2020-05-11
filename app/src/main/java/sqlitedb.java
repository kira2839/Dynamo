
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class sqlitedb extends SQLiteOpenHelper {
    private static final String
            DB_NAME="SimpleDynamo";
    public sqlitedb(Context context){
        super(context,DB_NAME,null,1);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE SimpleDynamo ('key' STRING PRIMARY KEY, value STRING, version int);");//changed from primarykey to string
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        //db.execSQL("DROP TABLE IF EXISTS SimpleDynamo");
        //onCreate(db);
    }
    public void addrow(ContentValues values){
        getWritableDatabase().insertWithOnConflict("SimpleDynamo","key",values,SQLiteDatabase.CONFLICT_REPLACE);
    }
    //to display the sqlite database has been referred from stackoverflow.
    public String getTableAsString(sqlitedb db, String tableName) {
        Log.d(TAG, "getTableAsString called");
        String tableString = String.format("Table %s:\n", tableName);
        String[] x = {"key0"};
        Cursor allRows  = db.getReadableDatabase().rawQuery("select * from SimpleDynamo", null);
        if (allRows.moveToFirst() ){
            String[] columnNames = allRows.getColumnNames();
            do {
                for (String name: columnNames) {
                    tableString += String.format("%s: %s\n", name,
                            allRows.getString(allRows.getColumnIndex(name)));
                }
                tableString += "\n";

            } while (allRows.moveToNext());
        }

        return tableString;
    }

    public int delete(String table, String whereClause, String[] whereArgs){
        return getWritableDatabase().delete("SimpleDynamo", "key"+":"+ whereClause, null);
    }
}


