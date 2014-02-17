/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.mongodb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Handler for schema operations with MongoDB datastores.
 */
public class MongoDBSchemaHandler extends AbstractStoreSchemaHandler
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.mongodb.Localisation", MongoDBStoreManager.class.getClassLoader());

    public MongoDBSchemaHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#createSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        DB db = (DB)connection;
        ManagedConnection mconn = null;
        try
        {
            if (db == null)
            {
                mconn = storeMgr.getConnection(-1);
                db = (DB)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    createSchemaForClass(cmd, db);
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    protected void createSchemaForClass(AbstractClassMetaData cmd, DB db)
    {
        String collectionName = storeMgr.getNamingFactory().getTableName(cmd);
        DBCollection collection = null;
        if (isAutoCreateTables())
        {
            // Create collection (if not existing)
            if (cmd.hasExtension(MongoDBStoreManager.CAPPED_SIZE_EXTENSION_NAME))
            {
                Set<String> collNames = db.getCollectionNames();
                if (!collNames.contains(collectionName))
                {
                    // Collection specified as "capped" with a size and doesn't exist so create it
                    DBObject options = new BasicDBObject();
                    options.put("capped", "true");
                    Long size = Long.valueOf(cmd.getValueForExtension(MongoDBStoreManager.CAPPED_SIZE_EXTENSION_NAME));
                    options.put("size", size);
                    db.createCollection(collectionName, options);
                }
                else
                {
                    collection = db.getCollection(collectionName);
                }
            }
            else
            {
                collection = db.getCollection(collectionName);
            }
        }

        if (autoCreateConstraints)
        {
            // Create indexes
            if (collection == null && !db.getCollectionNames().contains(collectionName))
            {
                NucleusLogger.DATASTORE_SCHEMA.warn("Cannot create constraints for " + cmd.getFullClassName() +
                    " since collection of name " + collectionName + " doesn't exist (enable autoCreateTables?)");
                return;
            }
            else if (collection == null)
            {
                collection = db.getCollection(collectionName);
            }

            // TODO Indexes at class-level for persistable superclasses
            IndexMetaData[] idxmds = cmd.getIndexMetaData();
            if (idxmds != null && idxmds.length > 0)
            {
                for (int i=0;i<idxmds.length;i++)
                {
                    DBObject idxObj = getDBObjectForIndex(cmd, idxmds[i]);
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("MongoDB.SchemaCreate.Class.Index",
                            idxmds[i].getName(), collectionName, idxObj));
                    }
                    collection.ensureIndex(idxObj, idxmds[i].getName(), idxmds[i].isUnique());
                }
            }
            UniqueMetaData[] unimds = cmd.getUniqueMetaData();
            if (unimds != null && unimds.length > 0)
            {
                for (int i=0;i<unimds.length;i++)
                {
                    DBObject uniObj = getDBObjectForUnique(cmd, unimds[i]);
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("MongoDB.SchemaCreate.Class.Index",
                            unimds[i].getName(), collectionName, uniObj));
                    }
                    collection.ensureIndex(uniObj, unimds[i].getName(), true);
                }
            }

            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                // Add unique index on PK
                BasicDBObject query = new BasicDBObject();
                int[] pkFieldNumbers = cmd.getPKMemberPositions();
                boolean applyIndex = true;
                for (int i=0;i<pkFieldNumbers.length;i++)
                {
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                    if (storeMgr.isStrategyDatastoreAttributed(cmd, pkFieldNumbers[i]))
                    {
                        applyIndex = false;
                        break;
                    }
                    query.append(storeMgr.getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN), 1);
                }
                if (applyIndex)
                {
                    String pkName = (cmd.getPrimaryKeyMetaData() != null ? cmd.getPrimaryKeyMetaData().getName() : cmd.getName() + "_PK");
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("MongoDB.SchemaCreate.Class.Index",
                            pkName, collectionName, query));
                    }
                    collection.ensureIndex(query, pkName, true);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                if (storeMgr.isStrategyDatastoreAttributed(cmd, -1))
                {
                    // Using builtin "_id" field so nothing to do
                }
                else
                {
                    BasicDBObject query = new BasicDBObject();
                    query.append(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN), 1);
                    String pkName = (cmd.getPrimaryKeyMetaData() != null ? cmd.getPrimaryKeyMetaData().getName() : cmd.getName() + "_PK");
                    if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("MongoDB.SchemaCreate.Class.Index",
                            pkName, collectionName, query));
                    }
                    collection.ensureIndex(query, pkName, true);
                }
            }

            AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
            if (mmds != null && mmds.length > 0)
            {
                for (int i=0;i<mmds.length;i++)
                {
                    String colName = storeMgr.getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN);
                    IndexMetaData idxmd = mmds[i].getIndexMetaData();
                    if (idxmd != null)
                    {
                        BasicDBObject query = new BasicDBObject();
                        query.append(colName, 1);
                        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("MongoDB.SchemaCreate.Class.Index",
                                idxmd.getName(), collectionName, query));
                        }
                        collection.ensureIndex(query, idxmd.getName(), idxmd.isUnique());
                    }
                    UniqueMetaData unimd = mmds[i].getUniqueMetaData();
                    if (unimd != null)
                    {
                        BasicDBObject query = new BasicDBObject();
                        query.append(colName, 1);
                        if (NucleusLogger.DATASTORE_SCHEMA.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("MongoDB.SchemaCreate.Class.Index",
                                unimd.getName(), collectionName, query));
                        }
                        collection.ensureIndex(query, unimd.getName(), true);
                    }
                }
            }
        }
    }

    private DBObject getDBObjectForIndex(AbstractClassMetaData cmd, IndexMetaData idxmd)
    {
        BasicDBObject idxObj = new BasicDBObject();
        if (idxmd.getNumberOfColumns() > 0)
        {
            String[] idxcolNames = idxmd.getColumnNames();
            for (int j=0;j<idxcolNames.length;j++)
            {
                idxObj.append(idxcolNames[j], 1);
            }
        }
        else if (idxmd.getNumberOfMembers() > 0)
        {
            String[] idxMemberNames = idxmd.getMemberNames();
            for (int i=0;i<idxMemberNames.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(idxMemberNames[i]);
                idxObj.append(storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), 1);
            }
        }
        return idxObj;
    }

    private DBObject getDBObjectForUnique(AbstractClassMetaData cmd, UniqueMetaData unimd)
    {
        BasicDBObject uniObj = new BasicDBObject();
        if (unimd.getNumberOfColumns() > 0)
        {
            String[] unicolNames = unimd.getColumnNames();
            for (int j=0;j<unicolNames.length;j++)
            {
                uniObj.append(unicolNames[j], 1);
            }
        }
        else if (unimd.getMemberNames() != null)
        {
            String[] uniMemberNames = unimd.getMemberNames();
            for (int i=0;i<uniMemberNames.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(uniMemberNames[i]);
                uniObj.append(storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), 1);
            }
        }
        return uniObj;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#deleteSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void deleteSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        DB db = (DB)connection;
        ManagedConnection mconn = null;
        try
        {
            if (db == null)
            {
                mconn = storeMgr.getConnection(-1);
                db = (DB)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    DBCollection collection = db.getCollection(storeMgr.getNamingFactory().getTableName(cmd));
                    collection.dropIndexes();
                    collection.drop();
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#validateSchema(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props, Object connection)
    {
        boolean success = true;
        DB db = (DB)connection;
        ManagedConnection mconn = null;
        try
        {
            if (db == null)
            {
                mconn = storeMgr.getConnection(-1);
                db = (DB)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    // Validate the schema for the class
                    String tableName = storeMgr.getNamingFactory().getTableName(cmd);
                    if (!db.collectionExists(tableName))
                    {
                        success = false;
                        String msg = "Table doesn't exist for " + cmd.getFullClassName() +
                            " - should have name="+ tableName;
                        System.out.println(msg);
                        NucleusLogger.DATASTORE_SCHEMA.error(msg);
                        continue;
                    }
                    else
                    {
                        String msg = "Table for class="+cmd.getFullClassName() + " with name="+tableName + " validated";
                        NucleusLogger.DATASTORE_SCHEMA.info(msg);
                    }

                    DBCollection table = db.getCollection(tableName);
                    List<DBObject> indices = new ArrayList(table.getIndexInfo());

                    IndexMetaData[] idxmds = cmd.getIndexMetaData();
                    if (idxmds != null && idxmds.length > 0)
                    {
                        for (int i=0;i<idxmds.length;i++)
                        {
                            DBObject idxObj = getDBObjectForIndex(cmd, idxmds[i]);
                            DBObject indexObj = getIndexObjectForIndex(indices, idxmds[i].getName(), idxObj, true);
                            if (indexObj != null)
                            {
                                indices.remove(indexObj);
                                String msg = "Index for class="+cmd.getFullClassName() + " with name="+idxmds[i].getName() + " validated";
                                NucleusLogger.DATASTORE_SCHEMA.info(msg);
                            }
                            else
                            {
                                success = false;
                                String msg = "Index missing for class="+cmd.getFullClassName() + " name="+idxmds[i].getName() + " key="+idxObj;
                                System.out.println(msg);
                                NucleusLogger.DATASTORE_SCHEMA.error(msg);
                            }
                        }
                    }
                    UniqueMetaData[] unimds = cmd.getUniqueMetaData();
                    if (unimds != null && unimds.length > 0)
                    {
                        for (int i=0;i<unimds.length;i++)
                        {
                            DBObject uniObj = getDBObjectForUnique(cmd, unimds[i]);
                            DBObject indexObj = getIndexObjectForIndex(indices, unimds[i].getName(), uniObj, true);
                            if (indexObj != null)
                            {
                                indices.remove(indexObj);
                                String msg = "Unique index for class="+cmd.getFullClassName() + " with name="+unimds[i].getName() + " validated";
                                NucleusLogger.DATASTORE_SCHEMA.info(msg);
                            }
                            else
                            {
                                success = false;
                                String msg = "Unique index missing for class="+cmd.getFullClassName() + " name="+unimds[i].getName() + " key="+uniObj;
                                System.out.println(msg);
                                NucleusLogger.DATASTORE_SCHEMA.error(msg);
                            }
                        }
                    }

                    if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        // Check unique index on PK
                        BasicDBObject query = new BasicDBObject();
                        int[] pkFieldNumbers = cmd.getPKMemberPositions();
                        for (int i=0;i<pkFieldNumbers.length;i++)
                        {
                            AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                            query.append(storeMgr.getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN), 1);
                        }
                        String pkName = (cmd.getPrimaryKeyMetaData() != null ? cmd.getPrimaryKeyMetaData().getName() : cmd.getName() + "_PK");
                        DBObject indexObj = getIndexObjectForIndex(indices, pkName, query, true);
                        if (indexObj != null)
                        {
                            indices.remove(indexObj);
                            String msg = "Index for application-identity with name="+pkName + " validated";
                            NucleusLogger.DATASTORE_SCHEMA.info(msg);
                        }
                        else
                        {
                            success = false;
                            String msg = "Index missing for application id name="+pkName + " key="+query;
                            System.out.println(msg);
                            NucleusLogger.DATASTORE_SCHEMA.error(msg);
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        if (storeMgr.isStrategyDatastoreAttributed(cmd, -1))
                        {
                            // Using builtin "_id" field so nothing to do
                        }
                        else
                        {
                            // Check unique index on PK
                            BasicDBObject query = new BasicDBObject();
                            query.append(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN), 1);
                            String pkName = (cmd.getPrimaryKeyMetaData() != null ? cmd.getPrimaryKeyMetaData().getName() : cmd.getName() + "_PK");
                            DBObject indexObj = getIndexObjectForIndex(indices, pkName, query, true);
                            if (indexObj != null)
                            {
                                indices.remove(indexObj);
                                String msg = "Index for datastore-identity with name="+pkName + " validated";
                                NucleusLogger.DATASTORE_SCHEMA.info(msg);
                            }
                            else
                            {
                                success = false;
                                String  msg = "Index missing for datastore id name="+pkName + " key="+query;
                                System.out.println(msg);
                                NucleusLogger.DATASTORE_SCHEMA.error(msg);
                            }
                        }
                    }

                    AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
                    if (mmds != null && mmds.length > 0)
                    {
                        for (int i=0;i<mmds.length;i++)
                        {
                            IndexMetaData idxmd = mmds[i].getIndexMetaData();
                            if (idxmd != null)
                            {
                                BasicDBObject query = new BasicDBObject();
                                query.append(storeMgr.getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN), 1);
                                DBObject indexObj = getIndexObjectForIndex(indices, idxmd.getName(), query, true);
                                if (indexObj != null)
                                {
                                    String msg = "Index for field=" + mmds[i].getFullFieldName() + " with name="+idxmd.getName() + " validated";
                                    NucleusLogger.DATASTORE_SCHEMA.info(msg);
                                    indices.remove(indexObj);
                                }
                                else
                                {
                                    success = false;
                                    String msg = "Index missing for field="+mmds[i].getFullFieldName() + " name="+idxmd.getName() + " key="+query;
                                    System.out.println(msg);
                                    NucleusLogger.DATASTORE_SCHEMA.error(msg);
                                }
                            }
                            UniqueMetaData unimd = mmds[i].getUniqueMetaData();
                            if (unimd != null)
                            {
                                BasicDBObject query = new BasicDBObject();
                                query.append(storeMgr.getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN), 1);
                                DBObject indexObj = getIndexObjectForIndex(indices, unimd.getName(), query, true);
                                if (indexObj != null)
                                {
                                    String msg = "Unique index for field=" + mmds[i].getFullFieldName() + " with name="+unimd.getName() + " validated";
                                    NucleusLogger.DATASTORE_SCHEMA.info(msg);
                                    indices.remove(indexObj);
                                }
                                else
                                {
                                    success = false;
                                    String msg = "Unique index missing for field="+mmds[i].getFullFieldName() + " name="+unimd.getName() + " key="+query;
                                    System.out.println(msg);
                                    NucleusLogger.DATASTORE_SCHEMA.error(msg);
                                }
                            }
                        }
                    }

                    // TODO Could extend this and check that we account for all MongoDB generated indices too
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }

        if (!success)
        {
            throw new NucleusException("Errors were encountered during validation of MongoDB schema");
        }
    }

    private DBObject getIndexObjectForIndex(List<DBObject> indices, String idxName, DBObject idxObj, boolean unique)
    {
        if (indices == null || indices.isEmpty())
        {
            return null;
        }

        Iterator<DBObject> idxIter = indices.iterator();
        while (idxIter.hasNext())
        {
            DBObject index = idxIter.next();
            DBObject obj = null;
            String name = (String) index.get("name");
            if (name.equals(idxName))
            {
                obj = index;
                if (unique)
                {
                    boolean flag = (Boolean) index.get("unique");
                    if (!flag)
                    {
                        continue;
                    }
                }

                boolean equal = true;
                DBObject key = (DBObject)index.get("key");
                if (key.toMap().size() != idxObj.toMap().size())
                {
                    equal = false;
                }
                else
                {
                    Iterator<String> indicKeyIter = key.keySet().iterator();
                    while (indicKeyIter.hasNext())
                    {
                        String fieldKey = indicKeyIter.next();
                        Object fieldValue = key.get(fieldKey);
                        if (!idxObj.containsField(fieldKey))
                        {
                            equal = false;
                        }
                        else
                        {
                            Object idxObjValue = idxObj.get(fieldKey);
                            if (!idxObjValue.equals(fieldValue))
                            {
                                equal = false;
                            }
                        }
                    }
                }
                if (equal)
                {
                    return obj;
                }
            }
        }
        return null;
    }
}