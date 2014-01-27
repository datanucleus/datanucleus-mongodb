/**********************************************************************
Copyright (c) 2011 Andy Jefferson. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.UniqueMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDBStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.mongodb.Localisation", MongoDBStoreManager.class.getClassLoader());

    public static final String CAPPED_SIZE_EXTENSION_NAME = "mongodb.capped.size";

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public MongoDBStoreManager(ClassLoaderResolver clr, NucleusContext nucleusCtx, Map<String, Object> props)
    {
        super("mongodb", clr, nucleusCtx, props);

        // Handler for persistence process
        persistenceHandler = new MongoDBPersistenceHandler(this);

        logConfiguration();
    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("NonDurableIdentity");
        set.add("ORM");
        set.add("TransactionIsolationLevel.read-committed");
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java.lang.Object, org.datanucleus.ClassLoaderResolver, org.datanucleus.store.ExecutionContext)
     */
    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec)
    {
        if (id == null)
        {
            // User stupidity
            return null;
        }
        else if (id instanceof SCOID)
        {
            // Object is a SCOID
            return ((SCOID) id).getSCOClass();
        }

        String rootClassName = super.getClassNameForObjectID(id, clr, ec);
        // TODO Allow for use of users-own PK class in multiple inheritance trees
        String[] subclasses = getMetaDataManager().getSubclassesForClass(rootClassName, true);
        if (subclasses == null || subclasses.length == 0)
        {
            // No subclasses so no need to go to the datastore
            return rootClassName;
        }

        AbstractClassMetaData rootCmd = getMetaDataManager().getMetaDataForClass(rootClassName, clr);
        return MongoDBUtils.getClassNameForIdentity(id, rootCmd, ec, clr);
    }

    /**
     * Accessor for whether this value strategy is supported.
     * Overrides the superclass to allow for "IDENTITY" since we support it and no entry in plugins for it.
     * @param strategy The strategy
     * @return Whether it is supported.
     */
    public boolean supportsValueStrategy(String strategy)
    {
        if (super.supportsValueStrategy(strategy))
        {
            return true;
        }

        // "identity" doesn't have an explicit entry in plugin since uses datastore capabilities
        if (strategy.equalsIgnoreCase("IDENTITY"))
        {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
	 * @see org.datanucleus.store.AbstractStoreManager#getStrategyForNative(org.datanucleus.metadata.AbstractClassMetaData, int)
	 */
	@Override
	protected String getStrategyForNative(AbstractClassMetaData cmd, int absFieldNumber)
	{
        if (absFieldNumber >= 0)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(absFieldNumber);
            Class type = mmd.getType();
            if (String.class.isAssignableFrom(type))
            {
                if (supportsValueStrategy("identity"))
                {
                    return "identity";
                }
                else
                {
                    return "uuid-hex";
                }
            }
            else if (type == Long.class || type == Integer.class || type == Short.class || 
                type == long.class || type == int.class || type == short.class)
            {
                if (supportsValueStrategy("sequence") && mmd.getSequence() != null)
                {
                    return "sequence";
                }
                else if (supportsValueStrategy("increment"))
                {
                    return "increment";
                }
                throw new NucleusUserException("This datastore provider doesn't support numeric native strategy for member " + mmd.getFullFieldName());
            }
            else
            {
                throw new NucleusUserException("This datastore provider doesn't support native strategy for field of type " + type.getName());
            }
        }
        else
        {
            IdentityMetaData idmd = cmd.getBaseIdentityMetaData();
            if (idmd != null && idmd.getColumnMetaData() != null)
            {
                String jdbcType = idmd.getColumnMetaData().getJdbcType();
                if (MetaDataUtils.isJdbcTypeString(jdbcType))
                {
                    return "uuid-hex";
                }
            }

            if (supportsValueStrategy("identity"))
            {
                return "identity";
            }
            else if (supportsValueStrategy("sequence") && idmd.getSequence() != null)
            {
                return "sequence";
            }
            else if (supportsValueStrategy("increment"))
            {
                return "increment";
            }
            throw new NucleusUserException("This datastore provider doesn't support numeric native strategy for class " + cmd.getFullClassName());
        }
	}

	/* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#addClasses(java.lang.String[], org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void addClasses(String[] classNames, ClassLoaderResolver clr)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = getConnection(-1);
        try
        {
            DB db = (DB)mconn.getConnection();

            addClasses(classNames, clr, db);
        }
        finally
        {
            mconn.release();
        }
    }

    public void addClasses(String[] classNames, ClassLoaderResolver clr, DB db)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = 
            getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE)
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        registerStoreData(newStoreData(cmd, clr));
                    }

                    // Create schema for class
                    createSchemaForClass(cmd, db);
                }
            }
        }
    }

    public void createSchema(String schemaName, Properties props)
    {
        throw new UnsupportedOperationException("Dont support the creation of a schema with MongoDB");
    }

    public void deleteSchema(String schemaName, Properties props)
    {
        throw new UnsupportedOperationException("Dont support the deletion of a schema with MongoDB");
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            DB db = (DB)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    createSchemaForClass(cmd, db);
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    protected void createSchemaForClass(AbstractClassMetaData cmd, DB db)
    {
        String collectionName = getNamingFactory().getTableName(cmd);
        DBCollection collection = null;
        if (autoCreateTables)
        {
            // Create collection (if not existing)
            if (cmd.hasExtension(CAPPED_SIZE_EXTENSION_NAME))
            {
                Set<String> collNames = db.getCollectionNames();
                if (!collNames.contains(collectionName))
                {
                    // Collection specified as "capped" with a size and doesn't exist so create it
                    DBObject options = new BasicDBObject();
                    options.put("capped", "true");
                    Long size = Long.valueOf(cmd.getValueForExtension(CAPPED_SIZE_EXTENSION_NAME));
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
                    if (isStrategyDatastoreAttributed(cmd, pkFieldNumbers[i]))
                    {
                        applyIndex = false;
                        break;
                    }
                    query.append(getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN), 1);
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
                if (isStrategyDatastoreAttributed(cmd, -1))
                {
                    // Using builtin "_id" field so nothing to do
                }
                else
                {
                    BasicDBObject query = new BasicDBObject();
                    query.append(getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN), 1);
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
                    String colName = getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN);
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
        if (idxmd.getColumnMetaData() != null)
        {
            ColumnMetaData[] idxcolmds = idxmd.getColumnMetaData();
            for (int j=0;j<idxcolmds.length;j++)
            {
                idxObj.append(idxcolmds[j].getName(), 1);
            }
        }
        else if (idxmd.getMemberNames() != null)
        {
            String[] idxMemberNames = idxmd.getMemberNames();
            for (int i=0;i<idxMemberNames.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(idxMemberNames[i]);
                idxObj.append(getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), 1);
            }
        }
        return idxObj;
    }

    private DBObject getDBObjectForUnique(AbstractClassMetaData cmd, UniqueMetaData unimd)
    {
        BasicDBObject uniObj = new BasicDBObject();
        if (unimd.getColumnMetaData() != null)
        {
            ColumnMetaData[] unicolmds = unimd.getColumnMetaData();
            for (int j=0;j<unicolmds.length;j++)
            {
                uniObj.append(unicolmds[j].getName(), 1);
            }
        }
        else if (unimd.getMemberNames() != null)
        {
            String[] uniMemberNames = unimd.getMemberNames();
            for (int i=0;i<uniMemberNames.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(uniMemberNames[i]);
                uniObj.append(getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), 1);
            }
        }
        return uniObj;
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            DB db = (DB)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    DBCollection collection = db.getCollection(getNamingFactory().getTableName(cmd));
                    collection.dropIndexes();
                    collection.drop();
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public void validateSchemaForClasses(Set<String> classNames, Properties props)
    {
        boolean success = true;
        ManagedConnection mconn = getConnection(-1);
        try
        {
            DB db = (DB)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    // Validate the schema for the class
                    String tableName = getNamingFactory().getTableName(cmd);
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
                            query.append(getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN), 1);
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
                        if (isStrategyDatastoreAttributed(cmd, -1))
                        {
                            // Using builtin "_id" field so nothing to do
                        }
                        else
                        {
                            // Check unique index on PK
                            BasicDBObject query = new BasicDBObject();
                            query.append(getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN), 1);
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
                                query.append(getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN), 1);
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
                                query.append(getNamingFactory().getColumnName(mmds[i], ColumnType.COLUMN), 1);
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
            mconn.release();
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