package com.database.db.core.manager;

import java.util.List;

import com.database.db.api.Condition.WhereClause;
import com.database.db.api.DatabaseException.ForeignKeyException;
import com.database.db.core.Database;
import com.database.db.core.Database.TableReference;
import com.database.db.core.page.Entry;
import com.database.db.core.table.Table;
import com.database.db.api.DBMS;
import com.database.db.api.UpdateFields;

public class ForeignKeyManager {
    public static void foreignKeyCheck(DBMS dbms, Database database, String tableName, Entry entry){
        Table childTable = database.getTable(tableName);
        TableReference reference = childTable.getParent();
        if (reference == null) return; // No FK, nothing to check
        // Loop through each FK column pair
        for (int i = 0; i < reference.parentColumns().size(); i++) {
            String childColumn = reference.childColumns().get(i);
            String parentColumn = reference.parentColumns().get(i);
            int columnIndex = childTable.getSchema().getColumnIndex(childColumn);
            Object[] values = entry.getEntry();
            Object value = values[columnIndex];
            // Check if the value exists in the parent table
            if (value == null) continue;
            if (!dbms.containsValue(reference.parentTable(), parentColumn, value)) {
                throw new ForeignKeyException(
                    reference.referenceName(),
                    "Insertion failed: value '" + value + "' for column '" + childColumn + 
                    "' does not exist in parent table '" + reference.parentTable() + 
                    "' column '" + parentColumn + "'."
                );
            }
        }
    }

    public static <K extends Comparable<? super K>> boolean foreignKeyDeletion(Table parentTable, Entry entry){
        List<TableReference> references = parentTable.getChildren();
        if(!ForeignKeyManager.canPerformAction(parentTable, entry, references)) return false;
        for (TableReference tableReference : references) {
            switch (tableReference.onDelete()) {
                case CASCADE -> { ForeignKeyManager.cascadeDelete(parentTable, entry, tableReference);}
                case SET_NULL -> { ForeignKeyManager.setNull(parentTable, entry, tableReference);}
                case SET_DEFAULT -> { ForeignKeyManager.setDeFault(parentTable, entry, tableReference);}
                case RESTRICT -> { continue; }
            };
        }
        return true;
    }

    public static <K extends Comparable<? super K>> boolean foreignKeyUpdate(Table parentTable, Entry entry, Object[] newValues){
        List<TableReference> references = parentTable.getChildren();
        if(!ForeignKeyManager.canPerformAction(parentTable, entry, references)) return false;
        for (TableReference tableReference : references) {
            switch (tableReference.onDelete()) {
                case CASCADE -> {ForeignKeyManager.cascadeUpdate(parentTable, entry, newValues, tableReference);}
                case SET_NULL -> { ForeignKeyManager.setNull(parentTable, entry, tableReference);}
                case SET_DEFAULT -> { ForeignKeyManager.setDeFault(parentTable, entry, tableReference);}
                case RESTRICT -> { continue; }
            };
        }
        return true;
    }

    private static <K extends Comparable<? super K>> void cascadeDelete(Table parentTable, Entry entry, TableReference reference) {
        Table childTable = parentTable.getDatabase().getTable(reference.childTable());
        WhereClause whereClause = new WhereClause();
        for (int i = 0; i < reference.childColumns().size(); i++) {
            int parentColumnIndex = parentTable.getSchema().getColumnIndex(reference.parentColumns().get(i));
            if (i == 0) {
                whereClause.column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            } else {
                whereClause.AND().column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }
        }
        EntryManager.deleteEntry(childTable, whereClause, -1);
    }

    private static <K extends Comparable<? super K>> void cascadeUpdate(Table parentTable, Entry entry, Object[] newValues, TableReference reference){
        Table childTable = parentTable.getDatabase().getTable(reference.childTable());
        WhereClause whereClause = new WhereClause();
        UpdateFields updateFields = new UpdateFields();
        for (int i = 0; i < reference.childColumns().size(); i++) {
            int parentColumnIndex = parentTable.getSchema().getColumnIndex(reference.parentColumns().get(i));
            if (i == 0) {
                whereClause.column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            } else {
                whereClause.AND().column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }
            updateFields.selectColumn(reference.childColumns().get(i));
            updateFields.set(newValues[parentColumnIndex]);
        }
        EntryManager.updateEntry(childTable, whereClause, -1, updateFields);
    }

    private static <K extends Comparable<? super K>> void setDeFault(Table parentTable, Entry entry, TableReference reference){
        Table childTable = parentTable.getDatabase().getTable(reference.childTable());
        UpdateFields updateFields = new UpdateFields();
        WhereClause whereClause = new WhereClause();
        for (int i = 0;i<reference.childColumns().size();i++) {
            int parentColumnIndex = parentTable.getSchema().getColumnIndex(reference.parentColumns().get(i));
            if(i == 0){ 
                whereClause.column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }else{ 
                whereClause.AND().column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }
            updateFields.selectColumn(reference.childColumns().get(i));
            updateFields.setDefault();
        }
        EntryManager.updateEntry(childTable, whereClause, -1, updateFields);
    }

    private static <K extends Comparable<? super K>> void setNull(Table parentTable, Entry entry, TableReference reference){
        Table childTable = parentTable.getDatabase().getTable(reference.childTable());
        UpdateFields updateFields = new UpdateFields();
        WhereClause whereClause = new WhereClause();
        for (int i = 0;i<reference.childColumns().size();i++) {
            int parentColumnIndex = parentTable.getSchema().getColumnIndex(reference.parentColumns().get(i));
            if(i == 0){ 
                whereClause.column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }else{ 
                whereClause.AND().column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }
            updateFields.selectColumn(reference.childColumns().get(i));
            updateFields.set(null);
        }
        EntryManager.updateEntry(childTable, whereClause, -1, updateFields);
    }

    private static <K extends Comparable<? super K>> boolean canPerformAction(Table parentTable, Entry entry, List<TableReference> references){
        boolean passed = true;
        for (TableReference tableReference : references) {
            Table childTable = parentTable.getDatabase().getTable(tableReference.childTable());
            passed = switch (tableReference.onDelete()) {
                case RESTRICT -> {
                    boolean allow = true;
                    for (int i = 0; i < tableReference.parentColumns().size(); i++) {
                        int parentColumnIndex = parentTable.getSchema().getColumnIndex(tableReference.parentColumns().get(i));
                        int childColumnsIndex = childTable.getSchema().getColumnIndex(tableReference.childColumns().get(i));
                        if (childTable.containsKey(entry.get(parentColumnIndex), childColumnsIndex)) {
                            allow = false;
                            break;
                        }
                    }
                    yield allow;
                }
                case CASCADE,SET_NULL,SET_DEFAULT -> { yield true; }
            };
            if (!passed) break;
        }
        return passed;
    }
}