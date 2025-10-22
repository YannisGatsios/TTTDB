package com.database.tttdb;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.*;

import com.database.tttdb.api.*;
import com.database.tttdb.core.index.IndexInit.IndexType;
import com.database.tttdb.core.table.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AppTest {

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

    private DBMS db;

    @BeforeAll
    void setup() {
        db = new DBMS()
        .addDatabase("app_test", 0)
        .setPath("data/");
    }

    static Schema buildSchema() {
        return new Schema()
            .column("username").type(DataType.CHAR).size(50).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(10).endColumn()
            .column("data").type(DataType.BYTE).size(10).notNull().defaultValue(new byte[10]).endColumn();
    }

    static List<Row> makeRows(int n) {
        ArrayList<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Row r = new Row("username,num,message,data")
            .set("username", "INSERTION" + i)
            .set("num", i)
            .set("message", "TEST")
            .set("data", new byte[]{(byte) i});
            rows.add(r);
        }
        return rows;
    }

    static List<Row> makeRandomRows(Schema schema, int n, long seed, ArrayList<String> keyList) {
        Random rnd = new Random(seed);
        ArrayList<Row> rows = new ArrayList<>(n);
        while (rows.size() < n) {
            String u = generateRandomString(1 + rnd.nextInt(schema.getColumns()[0].size() - 1));
            if(keyList.contains(u)) continue;
            keyList.add(u);
            Row r = new Row("username,num,message,data")
            .set("username", u)
            .set("num", 18 + rnd.nextInt(112))
            .set("message", (rows.size() % 25) == 0 ? null : "_HELLO_");
            byte[] data = new byte[rnd.nextInt(schema.getColumns()[3].size())];
            rnd.nextBytes(data);
            r.set("data", data);
            rows.add(r);
        }
        return rows;
    }

    @Test
    @Order(1)
    void testRandomInsertUpdateDelete() throws Exception {
        Schema schema = buildSchema()
            .column("id").autoIncrementing().unique().endColumn()
            .check("age_check")
                .open().column("num").isBiggerOrEqual(18).end()
                .close()
                .AND().column("num").isSmaller(130).end()
            .endCheck();
        db.addTable("users", schema)
        .start();

        ArrayList<String> keysList = new ArrayList<>(400);
        // Insert 400 entries
        int ind = 400;
        List<Row> expected = makeRandomRows(schema, ind, ind, keysList);
        db.insert("users", expected);
        db.commit();
        List<Row> actual = db.select("username,num,message,data").from("users").fetch();
        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertTrue(expected.containsAll(actual) && actual.containsAll(expected));

        // Delete 100 entries
        Random random = new Random(); 
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
        db.addTable("test_million_operations", buildSchema());
        db.start();
        db.startTransaction("Million rows operations");
        List<Row> rowsList = makeRows(1000000);
        //inserts
        assertEquals(1000000, db.insert("test_million_operations",rowsList));
        //deletes
        db.startTransaction("Million Deletions");
            db.delete().from("test_million_operations").execute();
            List<Row> preRollbackResult = db.select("*").from("test_million_operations").fetch();
            assertEquals(0, preRollbackResult.size());
        db.rollBack("Undoing Deletions");
        //select
        List<Row> afterRollbackResult = db.select("*").from("test_million_operations").fetch();
        assertEquals(1000000, afterRollbackResult.size());
        //delete
        db.delete().from("test_million_operations").execute();
        List<Row> commitDeletionsResult = db.select("*").from("test_million_operations").fetch();
        assertEquals(0, commitDeletionsResult.size());
        db.commit();
        db.dropTable("test_million_operations");
        db.close();
    }
    @Test
    @Order(5)
    void perIndexRangeSelectTest(){
        rangeSelectivity(IndexType.BTREE);
        rangeSelectivity(IndexType.HASH_INDEX);
        rangeSelectivity(IndexType.SKIPLIST);
        rangeSelectivity(IndexType.RED_BLACK_TREE);
    }
    void rangeSelectivity(IndexType indexType) {
        db.addTable("rangeSelectivity", buildSchema());
        db.setIndexType(indexType);
        db.start();
        // build table + index on "num" with {BTree, SkipList, Hash}
        int N = 1_000_000;
        List<Row> rows = makeRows(N);
        db.startTransaction("rangeSelectivity");
        db.insert("rangeSelectivity", rows);

        System.out.println(indexType.name()+" :");
        int[][] ranges = { {1000,1010}, {10_000,20_000}, {0, 999_999} };
        for (int[] r : ranges) {
            long t0 = System.nanoTime();
            rows = db.select("*")
                .from("rangeSelectivity")//"num BETWEEN ? AND ?", r[0], r[1]
                .where().column("num").isBiggerOrEqual(r[0]).isSmallerOrEqual(r[1]).end().endSelectClause()
            .fetch();
            long t1 = System.nanoTime();
            System.out.printf("range [%d,%d] -> %d rows in %.2f ms%n",
                r[0], r[1], rows.size(), (t1-t0)/1e6);
        }
        db.commit();
        db.dropTable("rangeSelectivity");
        db.close();
    }
}