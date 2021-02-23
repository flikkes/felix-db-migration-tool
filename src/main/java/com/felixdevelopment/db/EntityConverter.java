package com.felixdevelopment.db;

import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;

public class EntityConverter {
    private final String entityName;
    private final Map<String, Class<?>> fieldNameAndType;
    private final List<Document> entities;
    private int idSequence = 1;

    public EntityConverter(final List<Object> objects) throws IllegalAccessException {
        if (objects.size() < 1) {
            throw new IllegalArgumentException("There must be at least one object to read fields and data from!");
        }
        this.entityName = objects.get(0).getClass().getSimpleName();
        this.fieldNameAndType = new HashMap<>();
        Arrays.stream(objects.get(0).getClass().getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()) && !f.getName().equals("id"))
                .forEach(f -> this.fieldNameAndType.put(f.getName(), f.getType()));
        this.entities = new ArrayList<>();
        for (final Object obj : objects) {
            final Document document = new Document();
            for (final Field f : obj.getClass().getDeclaredFields()) {
                final int modifiers = f.getModifiers();
                if (!Modifier.isStatic(modifiers) && !f.getName().equals("id")) {
                    f.setAccessible(true);
                    document.put(f.getName(), f.get(obj));
                    f.setAccessible(false);
                }
            }
            this.entities.add(document);
        }
    }

    public EntityConverter(final List<Document> objects, final String collection) {
        if (objects.size() < 1) {
            throw new IllegalArgumentException("There must be at least one object to read fields and data from!");
        }
        this.fieldNameAndType = new HashMap<>();
        for (final Document obj : objects) {
            for (final String key : obj.keySet()) {
                if (!key.equals("id") && !key.equals("_id")) {
                    try {
                        obj.getInteger(key);
                        this.fieldNameAndType.put(key, Integer.class);
                        continue;
                    } catch (final ClassCastException e) {
                    }
                    try {
                        obj.getLong(key);
                        this.fieldNameAndType.put(key, Long.class);
                        continue;
                    } catch (final ClassCastException e) {
                    }
                    try {
                        obj.getDouble(key);
                        this.fieldNameAndType.put(key, Double.class);
                        continue;
                    } catch (final ClassCastException e) {
                    }
                    try {
                        obj.getBoolean(key);
                        this.fieldNameAndType.put(key, Boolean.class);
                        continue;
                    } catch (final ClassCastException e) {
                    }
                    try {
                        obj.getDate(key);
                        this.fieldNameAndType.put(key, Date.class);
                        continue;
                    } catch (final ClassCastException e) {
                    }
                    this.fieldNameAndType.put(key, String.class);
                }
            }
        }
        this.entityName = collection.trim().isEmpty() ? "Document" + System.currentTimeMillis() : collection;
        this.entities = objects;
    }

    private String getLegalTypeForMySQL(final Class<?> type) {

        if (type.equals(short.class) || type.equals(Short.class)) {
            return "SMALLINT";
        }
        if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            return "BOOLEAN";
        }
        if (type.equals(int.class) || type.equals(Integer.class)) {
            return "INT";
        }
        if (type.equals(long.class) || type.equals(Long.class)) {
            return "BIGINT";
        }
        if (type.equals(float.class) || type.equals(Float.class)) {
            return "FLOAT";
        }
        if (type.equals(double.class) || type.equals(Double.class)) {
            return "DOUBLE";
        }
        if (type.equals(LocalDate.class)) {
            return "DATE";
        }
        if (type.equals(Date.class) || type.equals(LocalDateTime.class)) {
            return "DATETIME";
        }
        return "VARCHAR(250)";
    }

    public List<Document> getMongoDBEntities() {
        return this.entities;
    }

    public void saveMongoDBEntities(final MongoTemplate template) {
        this.saveMongoDBEntities(template, MigrationStrategy.RENAME_OLD);
    }

    public void saveMongoDBEntities(final MongoTemplate template, MigrationStrategy migrationStrategy) {
        switch (migrationStrategy) {
        case REPLACE:
            template.dropCollection(this.entityName);
            template.insert(this.getMongoDBEntities(), this.entityName);
            break;
        case MERGE:
            template.insert(this.getMongoDBEntities(), this.entityName);
            break;
        case RENAME_OLD:
            final MongoCollection<Document> collection1 = template.getCollection(this.entityName);
            if (collection1.countDocuments() > 0) {
                collection1.renameCollection(new MongoNamespace(template.getDb().getName(),
                        this.entityName + "_OLD_" + System.currentTimeMillis()));
                template.insert(this.getMongoDBEntities(), this.entityName);
            } else {
                template.insert(this.getMongoDBEntities(), this.entityName);
            }
            break;
        case RENAME_NEW:
            final MongoCollection<Document> collection2 = template.getCollection(this.entityName);
            if (collection2.countDocuments() > 0) {
                template.insert(this.getMongoDBEntities(), this.entityName + "_NEW_" + System.currentTimeMillis());
            } else {
                template.insert(this.getMongoDBEntities(), this.entityName);
            }
            break;
        default:
            this.saveMongoDBEntities(template, MigrationStrategy.RENAME_OLD);
            break;
        }
    }

    public String createSQLQuery() {
        String query = "";
        query += "CREATE TABLE IF NOT EXISTS " + this.entityName + "(id INT PRIMARY KEY AUTO_INCREMENT, ";
        final Iterator<String> iterator = this.fieldNameAndType.keySet().iterator();
        final List<String> orderedKeys = new ArrayList<>();
        String placeholders = " (id, ";
        while (iterator.hasNext()) {
            final String fieldName = iterator.next();
            orderedKeys.add(fieldName);
            query += fieldName + " " + getLegalTypeForMySQL(this.fieldNameAndType.get(fieldName))
                    + (iterator.hasNext() ? ", " : "");
            placeholders += fieldName + (iterator.hasNext() ? "," : "");
        }
        query += ");\n";
        placeholders += ")";
        for (final Document document : this.entities) {

            query += "INSERT INTO " + this.entityName + placeholders + " VALUES " + "(" + this.idSequence + ", ";
            for (int i = 0; i < orderedKeys.size(); i++) {
                final String fieldName = orderedKeys.get(i);
                query += (this.fieldNameAndType.get(fieldName).equals(String.class)
                        ? "'" + document.get(fieldName).toString() + "'"
                        : document.get(fieldName).toString()) + "" + (i < orderedKeys.size() - 1 ? ", " : "");
            }
            query += ");\n";
            this.idSequence++;
        }
        this.idSequence = 1;
        System.out.println(query);
        return query;
    }

    public void saveSQLEntities(final JdbcTemplate template) {
        for (final String subQuery : this.createSQLQuery().split("\n")) {
            if (!subQuery.trim().isEmpty()) {
                template.execute(subQuery);
            }
        }
    }

    public String getEntityName() {
        return this.entityName;
    }

    public Map<String, Class<?>> getFieldNameAndType() {
        return this.fieldNameAndType;
    }

    public List<Document> getEntities() {
        return this.entities;
    }

}