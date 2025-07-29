package com.database.db;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;

import com.database.db.CRUD.updateFields;
import com.database.db.index.PrimaryKey;
import com.database.db.manager.EntryManager;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

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

    public record TableConfig(String tableName, Schema schema, int cacheCapacity){}

    public static void main(String[] args) throws IOException,InterruptedException, ExecutionException, Exception{
        //DataBase INIT.
        Database database = new Database("system");
        Schema schema = new Schema((
            "username:VARCHAR:10:PRIMARY_KEY:NULL;"+
            "num:INT:NON:INDEX:NULL;"+
            "message:VARCHAR:10:NO_CONSTRAINT:NULL;"+
            "data:BINARY:10:NOT_NULL:NON;"+
            "id:LONG:NON:AUTO_INCREMENT,UNIQUE:NULL")
            .split(";"));
        System.out.println(schema.toString());
        TableConfig tableConf = new TableConfig("users", schema, 10);

        database.createTable(tableConf);
        EntryManager crud = new EntryManager();
        crud.selectDatabase(database);
        crud.selectTable("users");

        Table table = database.getTable("users");
        //Tree INIT.
        
        Random random = new Random(); 
        ArrayList<String> keysList = new ArrayList<>(400);
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(schema.getSizes()[schema.getPrimaryKeyIndex()]-1)+1;
            String userName = generateRandomString(sizeOfID);
            if(!table.isKeyFound(userName,0)){
                keysList.add(userName);
                ArrayList<Object> entryData = new ArrayList<>();
                
                entryData.add(userName);
                int intNum = random.nextInt();
                entryData.add((intNum%25)==0 ? null : intNum);
                entryData.add((ind%25)==0 ? null : "_HELLO_");
                int sizeOfData = random.nextInt(table.getSchema().getSizes()[3]);
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                entryData.add(data);
                entryData.add(null);
                Entry entry = Entry.prepareEntry(entryData.toArray(),table);
                crud.insertEntry(entry);
                ind++;
            }
        }
        table.getCache().clear();
        PrimaryKey<String> tree2 = new PrimaryKey<>(table,0);
        int trues = 0;
        int falser = 0;
        for (String key : keysList) {
            if(tree2.isKey(key)){
                trues++;
            }else{
                falser++;
            }
        }
        //System.out.println("True Are : "+trues+"\nFalse Are : "+falser+"\n===========[Starting Random 100 Deletions.]===========");
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(400-ind);
            if(table.isKeyFound(keysList.get(randInd),0)){
                //System.out.println("("+keysList.get(randInd)+" : "+tree2.search(keysList.get(randInd))+")");
                String key = keysList.get(randInd);
                crud.deleteEntry(key,key, 0,1);
                //System.out.println("("+ind+") : Random Index : "+keysList.get(randInd)+" : "+tree2.search(keysList.get(randInd))+"\n========Deletion Finished=========\n");
                keysList.remove(randInd);
                ind++;
            }
        }
        table.getCache().clear();
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(300-ind);
            if(table.isKeyFound(keysList.get(randInd),0)){
                String key = keysList.get(randInd);
                crud.updateEntry(key, key, 0, -1, new updateFields()
                    .leftPad("username", 10, "x")
                    .set("data", new byte[10])
                    .getFunctionsList());
                keysList.remove(randInd);
                ind++;
            }
        }
        table.getCache().clear();
        database.removeTable("users");
        database.close();
    }
}
