package com.database.db;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.*;
import java.security.SecureRandom;
import java.util.*;

import com.database.db.api.*;
import com.database.db.api.Condition.*;
import com.database.db.api.DBMS.*;
import com.database.db.api.DBMS.Record;

import com.database.db.table.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AppTest {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = SECURE_RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

    private DBMS db;

    @BeforeAll
    void setup() {
        db = new DBMS("test_database", 10);
    }

    @Test
    @Order(1)
    void testRandomInsertUpdateDelete() throws Exception {
        Schema schema = new Schema()
            .column("username").type(DataType.CHAR).size(10).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(10).endColumn()
            .column("data").type(DataType.BYTE).size(10).notNull().defaultValue(new byte[10]).endColumn()
            .column("id").autoIncrementing().unique().endColumn()
            .check("age_check")
                .open().column("num").isBiggerOrEqual(18).end()
                .close()
                .AND().column("num").isSmaller(130).end()
            .endCheck();

        TableConfig tableConf = new TableConfig("users", schema);
        db.addTable(tableConf).create();

        Random random = new Random(); 
        ArrayList<String> keysList = new ArrayList<>(400);

        // Insert 400 entries
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(schema.getColumns()[0].size()-1);
            String userName = generateRandomString(sizeOfID);
            if(!db.containsValue("users", "username", userName)){
                ArrayList<Object> entryData = new ArrayList<>();
                int sizeOfData = random.nextInt(db.getColumnSizes("users")[3]);
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                keysList.add(userName);
                entryData.add(userName);
                entryData.add(ind%100+18);
                entryData.add((ind%25)==0 ? null : "_HELLO_");
                entryData.add(data);
                InsertQuery query = new InsertQuery("users", "username,num,message,data",entryData.toArray());
                db.insert(query);
                ind++;
            }
        }
        db.commit();

        // Delete 100 entries
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

        // Update 100 entries
        db.startTransaction("update transaction");
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(300-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                UpdateQuery query = new UpdateQuery("users",
                    new WhereClause().column("username").isEqual(key).end(), -1,
                    new UpdateFields("username")
                        .leftPad(10, "x")
                        .selectColumn("data")
                        .set(new byte[10]));
                db.update(query);
                keysList.remove(randInd);
                ind++;
            }
        }

        db.commit();

        // Select to verify
        SelectQuery query = new SelectQuery("users","id,username",null,0,-1);
        List<Record> result = db.select(query);
        Assertions.assertFalse(result.isEmpty(), "Users table should not be empty after insert/update/delete.");
        db.dropDatabase();
        db.close();
    }

    @Test
    @Order(2)
    void testForeignKeyActions() throws Exception {
        Schema userSchema = new Schema()
                .column("username").type(DataType.CHAR).size(15).primaryKey().endColumn()
                .column("age").type(DataType.INT).defaultValue(18).endColumn();

        TableConfig userTable = new TableConfig("users", userSchema);

        Schema postCascadeSchema = new Schema()
                .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
                .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
                .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
                .foreignKey("fk_user_cascade")
                    .column("username")
                    .reference().table("users").column("username").end()
                    .onDelete(ForeignKey.ForeignKeyAction.CASCADE)
                .endForeignKey();

        TableConfig postCascadeTable = new TableConfig("posts_cascade", postCascadeSchema);

        Schema postSetNullSchema = new Schema()
                .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
                .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
                .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
                .foreignKey("fk_user_setnull")
                    .column("username")
                    .reference().table("users").column("username").end()
                    .onDelete(ForeignKey.ForeignKeyAction.SET_NULL)
                .endForeignKey();

        TableConfig postSetNullTable = new TableConfig("posts_setnull", postSetNullSchema);

        Schema postSetDefaultSchema = new Schema()
                .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
                .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
                .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
                .foreignKey("fk_user_setdefault")
                    .column("username")
                    .reference().table("users").column("username").end()
                    .onDelete(ForeignKey.ForeignKeyAction.SET_DEFAULT)
                .endForeignKey();

        TableConfig postSetDefaultTable = new TableConfig("posts_setdefault", postSetDefaultSchema);

        Schema postRestrictSchema = new Schema()
                .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
                .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
                .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
                .foreignKey("fk_user_restrict")
                    .column("username")
                    .reference().table("users").column("username").end()
                    .onDelete(ForeignKey.ForeignKeyAction.RESTRICT)
                .endForeignKey();

        TableConfig postRestrictTable = new TableConfig("posts_restrict", postRestrictSchema);

        db.addDatabase("test_fk_cases",0);
        db.addTable(userTable);
        db.addTable(postCascadeTable);
        db.addTable(postSetNullTable);
        db.addTable(postSetDefaultTable);
        db.addTable(postRestrictTable);
        db.create().selectDatabase("test_fk_cases");

        db.insert(new InsertQuery("users", "username,age", new Object[]{"Alice", 25}));
        db.insert(new InsertQuery("users", "username,age", new Object[]{"Bob", 30}));
        db.commit();

        db.insert(new InsertQuery("posts_cascade", "username,content", new Object[]{"Alice","Hello Cascade"}));
        db.insert(new InsertQuery("posts_setnull", "username,content", new Object[]{"Bob","Bob Content"}));
        db.insert(new InsertQuery("posts_setdefault", "username,content", new Object[]{"Bob","Default Content"}));
        db.insert(new InsertQuery("posts_restrict", "username,content", new Object[]{"Alice","Restrict Content"}));
        db.commit();

        // Delete Alice
        try {
            db.delete(new DeleteQuery("users", new WhereClause().column("username").isEqual("Alice").end(), -1));
            db.commit();
        } catch (Exception e) {
            System.out.println("RESTRICT triggered: " + e.getMessage());
        }

        // Insert default user for SET_DEFAULT
        db.insert(new InsertQuery("users", "username,age", new Object[]{"DefaultUser", 0}));
        db.commit();

        // Delete Bob
        db.delete(new DeleteQuery("users", new WhereClause().column("username").isEqual("Bob").end(), -1));
        db.commit();

        // Verify state
        List<Record> remainingUsers = db.select(new SelectQuery("users", "username,age", null, 0, -1));
        Assertions.assertTrue(remainingUsers.stream().anyMatch(r -> r.get("username").equals("DefaultUser")),
            "DefaultUser should exist after SET_DEFAULT action");
        db.dropDatabase();
        db.close();
    }
}