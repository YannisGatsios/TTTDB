package com.database.tttdb;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

import com.database.tttdb.api.DBMS;
import com.database.tttdb.api.Row;
import com.database.tttdb.api.Schema;
import com.database.tttdb.core.index.IndexInit.IndexType;
import com.database.tttdb.core.table.DataType;

/**
 * End-to-end tests that run basic CRUD operations through the public DBMS API
 * for every available index implementation. This is the most important test
 * for the thesis claim that all structures can be plugged into the same DBMS.
 */
class DBMSIndexTypeIntegrationTest {

    @TempDir
    Path tempDir;

    @TestFactory
    Stream<DynamicTest> dbmsCrudUsesEveryIndexTypeCorrectly() {
        return Stream.of(IndexType.values()).map(type -> dynamicTest(type.name(), () -> {
            String databaseName = "db_" + type.name().toLowerCase();
            DBMS db = null;
            try {
                Schema schema = new Schema()
                    .column("username").type(DataType.CHAR).size(20).primaryKey().endColumn()
                    .column("age").type(DataType.INT).index().endColumn()
                    .column("message").type(DataType.CHAR).size(30).endColumn();

                db = new DBMS()
                    .addDatabase(databaseName, 10)
                    .setPath(tempDir.resolve(type.name()).toString())
                    .setIndexType(type)
                    .addTable("users", schema)
                    .start()
                    .selectDatabase(databaseName);

                db.insert("users", row("Alice", 25, "hello"));
                db.insert("users", row("Bob", 25, "world"));
                db.insert("users", row("Cara", 40, "test"));
                db.commit();

                assertTrue(db.containsValue("users", "username", "Alice"));
                assertTrue(db.containsValue("users", "age", 25));

                List<Row> age25 = db.select("username,age")
                    .from("users")
                    .where().column("age").isEqual(25).end().endSelectClause()
                    .fetch();
                assertEquals(2, age25.size(), "secondary index with duplicate key age=25 should return two rows");

                db.update("users")
                    .set()
                        .selectColumn("age").set(26)
                    .endUpdate()
                    .where().column("username").isEqual("Bob").end()
                    .endUpdateClause()
                    .execute();
                db.commit();

                assertEquals(1, db.select("username,age")
                    .from("users")
                    .where().column("age").isEqual(25).end().endSelectClause()
                    .fetch().size());

                assertEquals(1, db.select("username,age")
                    .from("users")
                    .where().column("age").isEqual(26).end().endSelectClause()
                    .fetch().size());

                db.delete()
                    .from("users")
                    .where().column("username").isEqual("Alice").end()
                    .endDeleteClause()
                    .execute();
                db.commit();

                assertFalse(db.containsValue("users", "username", "Alice"));
                assertEquals(2, db.select("username,age").from("users").fetch().size());
            } finally {
                if (db != null) {
                    try {
                        db.dropDatabase();
                    } catch (Exception ignored) {
                        // Best-effort cleanup; do not hide the real test failure.
                    }
                    try {
                        db.close();
                    } catch (Exception ignored) {
                        // Best-effort cleanup; do not hide the real test failure.
                    }
                }
            }
        }));
    }

    private static Row row(String username, int age, String message) {
        return new Row("username,age,message")
            .set("username", username)
            .set("age", age)
            .set("message", message);
    }
}
