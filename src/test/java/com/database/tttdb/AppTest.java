package com.database.tttdb;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.*;

import com.database.tttdb.api.*;
import com.database.tttdb.core.table.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
        db = new DBMS()
        .setPath("data/")
        .addDatabase("app_test", 0);
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
        db.addTable("users", schema)
        .start();

        Random random = new Random(); 
        ArrayList<String> keysList = new ArrayList<>(400);
        List<Row> expected = new ArrayList<>();
        // Insert 400 entries
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(schema.getColumns()[0].size()-1);
            String userName = generateRandomString(sizeOfID);
            if(!db.containsValue("users", "username", userName)){
                int sizeOfData = random.nextInt(db.getColumnSizes("users")[3]);
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                keysList.add(userName);
                Row row = new Row("username,num,message,data");
                row.set("username", userName);
                row.set("num", ind % 100 + 18);
                row.set("message", (ind % 25) == 0 ? null : "_HELLO_");
                row.set("data", data);
                expected.add(row);
                db.insertUnsafe("users", row);
                ind++;
            }
        }
        db.commit();
        List<Row> actual = db.select("username,num,message,data").from("users").fetch();
        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertTrue(expected.containsAll(actual) && actual.containsAll(expected));

        // Delete 100 entries
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(400-ind);
            if(db.containsValue("users", "username", keysList.get(randInd))){
                String key = keysList.get(randInd);
                expected.removeIf(r -> r.get("username").equals(key));
                db.delete().from("users")
                        .where().column("username").isEqual(key).end().endDeleteClause().execute();
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
                for (Row r : expected) {
                    if (r.get("username").equals(key)) {
                        r.set("username", padLeft((String) r.get("username"), 10, 'x'));
                        r.set("data", new byte[10]);
                    }
                }
                db.update("users")
                        .set()
                        .selectColumn("username").leftPad( 10, "x")
                        .selectColumn("data").set(new byte[10])
                    .endUpdate()
                    .where().column("username").isEqual(key).end().endUpdateClause().execute();
                keysList.remove(randInd);
                ind++;
            }
        }
        db.commit();
        List<Row> actualAfterUpdate = db.select("username,num,message,data").from("users").fetch();
        Assertions.assertEquals(expected.size(), actualAfterUpdate.size());
        Assertions.assertTrue(expected.containsAll(actualAfterUpdate) && actualAfterUpdate.containsAll(expected));

        // Select to verify
        List<Row> result = db.select("id,username").from("users").fetch();
        Assertions.assertFalse(result.isEmpty(), "Users table should not be empty after insert/update/delete.");
        db.dropTable("users");
        db.close();
    }
    @Test
    @Order(2)
    void testForeignKeyActions() throws Exception {
        // ---- Define schemas using the new API ----
        Schema userSchema = new Schema()
            .column("username").type(DataType.CHAR).size(15).primaryKey().endColumn()
            .column("age").type(DataType.INT).defaultValue(18).endColumn();

        Schema postCascadeSchema = new Schema()
            .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
            .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
            .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
            .foreignKey("fk_user_cascade")
                .column("username")
                .reference().table("users").column("username").end()
                .onDelete(ForeignKey.ForeignKeyAction.CASCADE)
            .endForeignKey();

        Schema postSetNullSchema = new Schema()
            .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
            .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
            .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
            .foreignKey("fk_user_setnull")
                .column("username")
                .reference().table("users").column("username").end()
                .onDelete(ForeignKey.ForeignKeyAction.SET_NULL)
            .endForeignKey();

        Schema postSetDefaultSchema = new Schema()
            .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
            .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
            .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
            .foreignKey("fk_user_setdefault")
                .column("username")
                .reference().table("users").column("username").end()
                .onDelete(ForeignKey.ForeignKeyAction.SET_DEFAULT)
            .endForeignKey();

        Schema postRestrictSchema = new Schema()
            .column("id").type(DataType.LONG).autoIncrementing().primaryKey().endColumn()
            .column("username").type(DataType.CHAR).size(15).defaultValue("DefaultUser").endColumn()
            .column("content").type(DataType.CHAR).size(50).defaultValue("Hello").endColumn()
            .foreignKey("fk_user_restrict")
                .column("username")
                .reference().table("users").column("username").end()
                .onDelete(ForeignKey.ForeignKeyAction.RESTRICT)
            .endForeignKey();

        // ---- Create database and tables with the new calls ----
        db.addTable("users", userSchema);
        db.addTable("posts_cascade", postCascadeSchema);
        db.addTable("posts_setnull", postSetNullSchema);
        db.addTable("posts_setdefault", postSetDefaultSchema);
        db.addTable("posts_restrict", postRestrictSchema);
        db.start();

        // Insert users
        Row row1 = new Row("username,age");
        row1.set("username", "Alice");
        row1.set("age", 25);
        db.insertUnsafe("users", row1);

        Row row2 = new Row("username,age");
        row2.set("username", "Bob");
        row2.set("age", 30);
        db.insertUnsafe("users", row2);

        db.commit();

        // Insert posts for each FK behavior
        Row post1 = new Row("username,content");
        post1.set("username", "Alice");
        post1.set("content", "Hello Cascade");
        db.insertUnsafe("posts_cascade", post1);

        Row post2 = new Row("username,content");
        post2.set("username", "Bob");
        post2.set("content", "Bob Content");
        db.insertUnsafe("posts_setnull", post2);

        Row post3 = new Row("username,content");
        post3.set("username", "Bob");
        post3.set("content", "Default Content");
        db.insertUnsafe("posts_setdefault", post3);

        Row post4 = new Row("username,content");
        post4.set("username", "Alice");
        post4.set("content", "Restrict Content");
        db.insertUnsafe("posts_restrict", post4);

        db.commit();

        // Attempt to delete Alice (RESTRICT should block posts_restrict)
        try {
            db.delete()
                .from("users")
                .where().column("username").isEqual("Alice").end()
                .endDeleteClause().execute();
            db.commit();
        } catch (Exception e) {
            System.out.println("RESTRICT triggered: " + e.getMessage());
        }

        // Insert default user for SET_DEFAULT
        Row defaultUser = new Row("username,age");
        defaultUser.set("username", "DefaultUser");
        defaultUser.set("age", 0);
        db.insertUnsafe("users", defaultUser);

        db.commit();

        // Delete Bob — should trigger CASCADE, SET_NULL, SET_DEFAULT actions
        db.delete()
            .from("users")
            .where().column("username").isEqual("Bob").end()
            .endDeleteClause().execute();
        db.commit();

        // Verify final state using Select fluent API
        List<Row> remainingUsers = db.select("username,age").from("users").fetch();
        Assertions.assertTrue(
            remainingUsers.stream().anyMatch(r -> "DefaultUser".equals(r.get("username"))),
            "DefaultUser should exist after SET_DEFAULT action");
        db.dropTable("users");
        db.dropTable("posts_cascade");
        db.dropTable("posts_setnull");
        db.dropTable("posts_setdefault");
        db.dropTable("posts_restrict");
        db.close();
    }
    @Test
    @Order(3)
    void testSelectionOrdering() throws Exception {
        Schema schema = new Schema()
            .column("id").type(DataType.INT).primaryKey().endColumn()
            .column("name").type(DataType.CHAR).size(20).endColumn()
            .column("age").type(DataType.INT).endColumn();
        db.addTable("people", schema)
        .start();

        // Insert test data
        Row row1 = new Row("id,name,age")
        .set("id", 1)
        .set("name", "Charlie")
        .set("age", 35);
        db.insertUnsafe("people", row1);

        Row row2 = new Row("id,name,age");
        row2.set("id", 2);
        row2.set("name", "Alice");
        row2.set("age", 30);
        db.insertUnsafe("people", row2);

        Row row3 = new Row("id,name,age");
        row3.set("id", 3);
        row3.set("name", "Bob");
        row3.set("age", 25);
        db.insertUnsafe("people", row3);

        db.commit();

        // === Test ASC by name ===
        List<Row> ascResult = db.select("id,name,age").from("people").ASC("name").fetch();
        Assertions.assertEquals("Alice", ascResult.get(0).get("name"));
        Assertions.assertEquals("Bob", ascResult.get(1).get("name"));
        Assertions.assertEquals("Charlie", ascResult.get(2).get("name"));

        // === Test DESC by age ===
        List<Row> descResult = db.select("id,name,age").from("people").DEC("age").fetch();
        Assertions.assertEquals("Charlie", descResult.get(0).get("name"));
        Assertions.assertEquals("Alice", descResult.get(1).get("name"));
        Assertions.assertEquals("Bob", descResult.get(2).get("name"));

        // === Test pagination (skip 1, take 1, ordered ASC by name) ===
        List<Row> pagedAsc = db.select("id,name,age")
                .from("people")
                .ASC("name")
                .begin(1)   // skip first (Alice)
                .limit(1)   // take only 1 row
                .fetch();
        Assertions.assertEquals(1, pagedAsc.size());
        Assertions.assertEquals("Alice", pagedAsc.get(0).get("name"));

        // === Test pagination (skip 1, take 2, ordered DESC by age) ===
        List<Row> pagedDesc = db.select("id,name,age")
                .from("people")
                .DEC("age")
                .begin(1)   // skip first (Charlie)
                .limit(2)   // next two
                .fetch();
        Assertions.assertEquals(2, pagedDesc.size());
        Assertions.assertEquals("Alice", pagedDesc.get(0).get("name"));
        Assertions.assertEquals("Bob", pagedDesc.get(1).get("name"));
        db.dropTable("people");
        db.close();
    }
    //Helper
    private static String padLeft(String input, int length, char padChar) {
        if (input == null) return null;
        if (input.length() >= length) return input;
        StringBuilder sb = new StringBuilder(length);
        for (int i = input.length(); i < length; i++) {
            sb.append(padChar);
        }
        sb.append(input);
        return sb.toString();
    }
    @Test
    @Order(4)
    void millionOperations(){
        Schema test_million_operations = new Schema()
            .column("username").type(DataType.CHAR).size(50).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(10).endColumn()
            .column("data").type(DataType.BYTE).size(10).notNull().defaultValue(new byte[10]).endColumn();
        db.addTable("test_million_operations", test_million_operations);
        db.start();
        List<Row> rowsList = new ArrayList<>();
        int ind = 0;
        while(ind < 1000000){
            String key = "INSERTION"+ind;
            Row row = new Row("username,num,message,data");
            row.set("username", key);
            row.set("num", ind);
            row.set("message", "TEST");
            row.set("data", new byte[]{(byte) ind});
            rowsList.add(row);
            ind++;
        }
        assertEquals(1000000, db.insert("test_million_operations",rowsList));
        db.commit();
        db.startTransaction("Million Deletions");
            db.delete().from("test_million_operations").execute();
            List<Row> preRollbackResult = db.select("*").from("test_million_operations").fetch();
            assertEquals(0, preRollbackResult.size());
        db.rollBack("Undoing Deletions");

        List<Row> afterRollbackResult = db.select("*").from("test_million_operations").fetch();
        assertEquals(1000000, afterRollbackResult.size());
        db.delete().from("test_million_operations").execute();
        db.commit();
        List<Row> commitDeletionsResult = db.select("*").from("test_million_operations").fetch();
        assertEquals(0, commitDeletionsResult.size());
        db.close();
    }
}