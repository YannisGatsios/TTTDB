package com.database.tttdb;

import java.util.Random;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.database.tttdb.api.DBMS.*;
import com.database.tttdb.api.Schema;
import com.database.tttdb.api.DatabaseException.EntryValidationException;
import com.database.tttdb.api.Query.Delete;
import com.database.tttdb.api.Query.Select;
import com.database.tttdb.api.Query.Update;
import com.database.tttdb.core.table.DataType;
import com.database.tttdb.api.DBMS;
import com.database.tttdb.api.Row;

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
            .column("date").type(DataType.TIMESTAMP).endColumn()
            .check("age_check")
                .open()
                    .column("num")
                    .isBiggerOrEqual(18).end()
                .close()
                .AND()
                .column("num")
                .isSmaller(130).end()
            .endCheck();

        TableConfig tableConf = new TableConfig("users", schema);
        DBMS db = new DBMS()
            .addDatabase("test_database", 0)
            .setPath("")
            .addTable(tableConf)
        .start()
        .selectDatabase("test_database");
        
        Random random = new Random(); 
        ArrayList<String> keysList = new ArrayList<>(400);
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(db.getColumnSizes("users")[0]-1)+1;
            String userName = generateRandomString(sizeOfID);
            if(!db.containsValue("users", "username", userName)){
                int sizeOfData = random.nextInt(db.getColumnSizes("users")[3]);
                //int intNum = random.nextInt();
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                keysList.add(userName);
                Row row = new Row("username,num,message,data");
                row.set("username", userName);
                row.set("num", ind%100+18);
                row.set("message", (ind%25)==0 ? null : "_HELLO_");
                row.set("data", data);
                db.insertUnsafe("users",row);
                ind++;
            }
        }
        db.commit();
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(400-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                Delete delete = (Delete)new Delete().from("users")
                .where().column("username").isEqual(key).end().endDeleteClause();
                db.delete(delete);
                keysList.remove(randInd);
                ind++;
            }
        }
        db.startTransaction("update transaction");
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(300-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                Update update = (Update)new Update("users")
                    .set()
                    .selectColumn("username").leftPad( 10, "x")
                    .selectColumn("data").set(new byte[10])
                .endUpdate()
                .where().column("username").isEqual(key).end().endUpdateClause();
                try{
                    db.update(update);
                }catch(EntryValidationException e){
                    db.rollBack();
                    e.printStackTrace();
                    break;
                }
                keysList.remove(randInd);
                ind++;
            }
        }
        db.commit();
        Select select = new Select("id,username,date").from("users");
        List<Row> result = db.select(select);
        db.printDatabase();
        db.dropDatabase();
        db.close();
    }
}