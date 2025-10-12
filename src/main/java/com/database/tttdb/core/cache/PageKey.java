package com.database.tttdb.core.cache;

import java.util.Objects;

public final class PageKey implements Comparable<PageKey> {
    private final String tableName;
    private final String columnName; // null for table pages
    private final int pageId;

    private PageKey(String tableName, String columnName, int pageId) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.pageId = pageId;
    }

    public static PageKey table(String tableName, int pageId) {
        return new PageKey(tableName, null, pageId);
    }

    public static PageKey index(String tableName, String columnName, int pageId) {
        return new PageKey(tableName, columnName, pageId);
    }

    public boolean isIndex() { return columnName != null; }
    public String getTableName() { return tableName; }
    public String getColumnName() { return columnName; }
    public int getPageId() { return pageId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PageKey)) return false;
        PageKey k = (PageKey) o;
        return pageId == k.pageId &&
               Objects.equals(tableName, k.tableName) &&
               Objects.equals(columnName, k.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnName, pageId);
    }

    @Override
    public String toString() {
        return columnName == null
            ? tableName + "." + pageId
            : tableName + "." + columnName + "." + pageId;
    }

    @Override
    public int compareTo(PageKey o) {
        int c1 = tableName.compareTo(o.tableName);
        if (c1 != 0) return c1;
        int c2 = String.valueOf(columnName).compareTo(String.valueOf(o.columnName));
        if (c2 != 0) return c2;
        return Integer.compare(pageId, o.pageId);
    }
}
