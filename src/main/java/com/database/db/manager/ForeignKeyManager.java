package com.database.db.manager;

import java.util.List;

import com.database.db.Database;
import com.database.db.Database.TableReference;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.DBMS;
import com.database.db.api.UpdateFields;
import com.database.db.page.Entry;
import com.database.db.table.Table;

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
                throw new IllegalStateException(
                    "Insertion failed: value '" + value + "' for column '" + childColumn + 
                    "' does not exist in parent table '" + reference.parentTable() + 
                    "' column '" + parentColumn + "'."
                );
            }
        }
    }

    public static <K extends Comparable<? super K>> boolean foreignKeyDeletion(EntryManager entryManager, Table parentTable, Entry entry){
        List<TableReference> references = parentTable.getChildren();
        if(!ForeignKeyManager.canPerformAction(entryManager, parentTable, entry, references)) return false;
        for (TableReference tableReference : references) {
            switch (tableReference.onDelete()) {
                case CASCADE -> { ForeignKeyManager.cascadeDelete(entryManager, parentTable, entry, tableReference);}
                case SET_NULL -> { ForeignKeyManager.setNull(entryManager, parentTable, entry, tableReference);}
                case SET_DEFAULT -> { ForeignKeyManager.setDeFault(entryManager, parentTable, entry, tableReference);}
                case RESTRICT -> { continue; }
            };
        }
        entryManager.selectTable(parentTable.getName());
        return true;
    }

    public static <K extends Comparable<? super K>> boolean foreignKeyUpdate(EntryManager entryManager, Table parentTable, Entry entry, Object[] newValues){
        List<TableReference> references = parentTable.getChildren();
        if(!ForeignKeyManager.canPerformAction(entryManager, parentTable, entry, references)) return false;
        for (TableReference tableReference : references) {
            switch (tableReference.onDelete()) {
                case CASCADE -> {ForeignKeyManager.cascadeUpdate(entryManager, parentTable, entry, newValues, tableReference);}
                case SET_NULL -> { ForeignKeyManager.setNull(entryManager, parentTable, entry, tableReference);}
                case SET_DEFAULT -> { ForeignKeyManager.setDeFault(entryManager, parentTable, entry, tableReference);}
                case RESTRICT -> { continue; }
            };
        }
        entryManager.selectTable(parentTable.getName());
        return true;
    }

    private static <K extends Comparable<? super K>> void cascadeDelete(EntryManager entryManager, Table parentTable, Entry entry, TableReference reference) {
        entryManager.selectTable(reference.childTable());
        WhereClause whereClause = new WhereClause();
        for (int i = 0; i < reference.childColumns().size(); i++) {
            int parentColumnIndex = parentTable.getSchema().getColumnIndex(reference.parentColumns().get(i));
            if (i == 0) {
                whereClause.column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            } else {
                whereClause.AND().column(reference.childColumns().get(i)).isEqual(entry.get(parentColumnIndex)).end();
            }
        }
        entryManager.deleteEntry(whereClause, -1);
    }

    private static <K extends Comparable<? super K>> void cascadeUpdate(EntryManager entryManager, Table parentTable, Entry entry, Object[] newValues, TableReference reference){
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
        entryManager.updateEntry(whereClause, -1, updateFields);
    }

    private static <K extends Comparable<? super K>> void setDeFault(EntryManager entryManager, Table parentTable, Entry entry, TableReference reference){
        entryManager.selectTable(reference.childTable());
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
        entryManager.updateEntry(whereClause, -1, updateFields);
    }

    private static <K extends Comparable<? super K>> void setNull(EntryManager entryManager, Table parentTable, Entry entry, TableReference reference){
        entryManager.selectTable(reference.childTable());
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
        entryManager.updateEntry(whereClause, -1, updateFields);
    }

    private static <K extends Comparable<? super K>> boolean canPerformAction(EntryManager entryManager, Table parentTable, Entry entry, List<TableReference> references){
        boolean passed = true;
        for (TableReference tableReference : references) {
            entryManager.selectTable(tableReference.childTable());
            passed = switch (tableReference.onDelete()) {
                case RESTRICT -> {
                    boolean allow = true;
                    for (int i = 0; i < tableReference.parentColumns().size(); i++) {
                        int parentColumnIndex = parentTable.getSchema().getColumnIndex(tableReference.parentColumns().get(i));
                        int childColumnsIndex = entryManager.getTable().getSchema().getColumnIndex(tableReference.childColumns().get(i));
                        if (entryManager.getTable().isKeyFound(entry.get(parentColumnIndex), childColumnsIndex)) {
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
        entryManager.selectTable(parentTable.getName());
        return passed;
    }
}