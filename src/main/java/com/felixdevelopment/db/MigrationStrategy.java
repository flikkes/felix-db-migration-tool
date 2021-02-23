package com.felixdevelopment.db;

/**
 * Describes how Data is written
 */
public enum MigrationStrategy {
    /**
     * If table or collection already exists, drop and replace it.
     */
    REPLACE,
    /**
     * Just add Data to collection/table if exists or create new collection/table if
     * not. May produce duplicate data if same datasets were already present in
     * collection/table.
     */
    MERGE,
    /**
     * If collection/table already exists and has data, rename it. Syntax:
     * &lt;entity_name&gt;_OLD_&lt;System.currentTimeMillis()&gt;
     */
    RENAME_OLD,
    /**
     * If collection/table already exists and has data, leave it and choose an
     * alternative collection/table name for the new data. Syntax:
     * &lt;entity_name&gt;_NEW_&lt;System.currentTimeMillis()&gt;
     */
    RENAME_NEW;
}
