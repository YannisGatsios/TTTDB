package com.database.db;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.database.db.api.DBMS.TableConfig;
import com.database.db.api.DBMS.DeleteQuery;
import com.database.db.api.DBMS.InsertQuery;
import com.database.db.api.DBMS.SelectQuery;
import com.database.db.api.DBMS.UpdateQuery;
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

    public static void main(String[] args) throws IOException,InterruptedException, ExecutionException, Exception{
        //DataBase INIT.
        String schemaConfig = 
            "username:CHAR:10:PRIMARY_KEY:NULL;"+
            "num:INT:NON:INDEX:NULL;"+
            "message:CHAR:10:NO_CONSTRAINT:NULL;"+
            "data:BYTE:10:NOT_NULL:NON;"+
            "id:LONG:NON:AUTO_INCREMENT,UNIQUE:NULL";
        TableConfig tableConf = new TableConfig("users", schemaConfig, 2);
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
                int intNum = random.nextInt();
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                keysList.add(userName);
                entryData.add(userName);
                entryData.add((intNum%25)==0 ? null : intNum);
                entryData.add((ind%25)==0 ? null : "_HELLO_");
                entryData.add(data);
                System.out.println("Insertion : "+ind);
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
                System.out.println("Deletion : "+ind);
                DeleteQuery query = new DeleteQuery("users", new WhereClause("username").isEqual(key),-1);
                db.delete(query);
                keysList.remove(randInd);
                ind++;
            }
        }
        db.commit();
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(300-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                UpdateQuery query = new UpdateQuery("users",
                new WhereClause("username").isEqual(key), -1,
                new UpdateFields("username")
                    .leftPad( 10, "x")
                    .selectColumn("data")
                    .set(new byte[10]));
                db.update(query);
                keysList.remove(randInd);
                ind++;
            }
        }
        SelectQuery query = new SelectQuery("users",new String[]{"id","username"},new WhereClause("username"),0,-1);
        db.commit();
        List<Record> result = db.select(query);
        db.dropDatabase();
        db.close();
    }
}
