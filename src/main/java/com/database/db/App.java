package com.database.db;

import java.util.Random;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.database.db.api.DBMS.*;
import com.database.db.table.DataType;
import com.database.db.api.Schema;
import com.database.db.api.DBMS.Record;
import com.database.db.api.DBMS;
import com.database.db.api.UpdateFields;
import com.database.db.api.Condition.WhereClause;

public class App {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = SECURE_RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        //DataBase INIT.
        Schema schema = new Schema()
            .column("username").type(DataType.CHAR).size(10).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(10).endColumn()
            .column("data").type(DataType.BYTE).size(10).notNull().defaultValue(new byte[10]).endColumn()
            .column("id").autoIncrementing().unique().endColumn()
            .check("age_check")
                .open()
                    .column("num")
                    .isBiggerOrEqual(18).end()
                .close()
                .AND()
                .column("num")
                .isSmaller(130).end()
            .endCheck();

        TableConfig tableConf = new TableConfig("users", schema, new CacheCapacity(0,0));
        DBMS db = new DBMS("test_database","")
            .createTable(tableConf)
            .selectDatabase("test_database");
        
        Random random = new Random(); 
        ArrayList<String> keysList = new ArrayList<>(400);
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(db.getColumnSizes("users")[0]-1)+1;
            String userName = generateRandomString(sizeOfID);
            if(!db.containsValue("users", "username", userName)){
                ArrayList<Object> entryData = new ArrayList<>();
                int sizeOfData = random.nextInt(db.getColumnSizes("users")[3]);
                //int intNum = random.nextInt();
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                keysList.add(userName);
                entryData.add(userName);
                entryData.add(ind%100+18);
                entryData.add((ind%25)==0 ? null : "_HELLO_");
                entryData.add(data);
                InsertQuery query = new InsertQuery("users", new String[]{"username","num","message","data"},entryData.toArray());
                db.insert(query);
                ind++;
            }
        }
        db.commit();
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(400-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                DeleteQuery query = new DeleteQuery("users", new WhereClause().column("username").isEqual(key).end(),-1);
                db.delete(query);
                keysList.remove(randInd);
                ind++;
            }
        }
        db.startTransaction();
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(300-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                UpdateQuery query = new UpdateQuery("users",
                new WhereClause().column("username").isEqual(key).end(), -1,
                new UpdateFields("username")
                    .leftPad( 10, "x")
                    .selectColumn("data")
                    .set(new byte[10]));
                db.update(query);
                keysList.remove(randInd);
                ind++;
            }
        }
        SelectQuery query = new SelectQuery("users",new String[]{"id","username"},null,0,-1);
        db.commit();
        List<Record> result = db.select(query);
        //db.dropDatabase();
        db.close();
    }
}