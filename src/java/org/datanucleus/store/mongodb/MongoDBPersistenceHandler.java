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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import org.bson.types.ObjectId;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.mongodb.fieldmanager.FetchFieldManager;
import org.datanucleus.store.mongodb.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

public class MongoDBPersistenceHandler extends AbstractPersistenceHandler
{
    protected static final Localiser LOCALISER_MONGODB = Localiser.getInstance(
            "org.datanucleus.store.mongodb.Localisation", MongoDBStoreManager.class.getClassLoader());

    public static final String OP_DB_OBJECT = "DB_OBJECT";

    public MongoDBPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
        this.storeMgr = (MongoDBStoreManager) storeMgr;
    }

    public void close()
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void insertObjects(ObjectProvider... ops)
    {
        // This is called by the flush() process, so groups inserts.
        List<ObjectProvider> insertOps = new ArrayList<ObjectProvider>();
        for (int i=0;i<ops.length;i++)
        {
            insertOps.add(ops[i]);
        }

        if (ops.length == 1)
        {
            insertObject(ops[0]);
            return;
        }

        // Process "identity" cases first in case they are referenced
        for (int i=0;i<ops.length;i++)
        {
            AbstractClassMetaData cmd = ops[i].getClassMetaData();
            if (cmd.pkIsDatastoreAttributed(storeMgr))
            {
                insertObject(ops[i]);
            }
        }

        // Separate the objects to be persisted into groups, for the "table" in question
        Map<String, Set<ObjectProvider>> opsByTable = new HashMap();
        for (int i=0;i<ops.length;i++)
        {
            AbstractClassMetaData cmd = ops[i].getClassMetaData();
            if (!cmd.pkIsDatastoreAttributed(storeMgr))
            {
                String tableName = storeMgr.getNamingFactory().getTableName(cmd);
                Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
                if (opsForTable == null)
                {
                    opsForTable = new HashSet<ObjectProvider>();
                    opsByTable.put(tableName, opsForTable);
                }
                opsForTable.add(ops[i]);
            }
        }

        for (String tableName : opsByTable.keySet())
        {
            Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
            ExecutionContext ec = ops[0].getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                DB db = (DB)mconn.getConnection();
                long startTime = System.currentTimeMillis();

                DBCollection collection = db.getCollection(tableName);
                DBObject[] dbObjects = new DBObject[opsForTable.size()];
                int i=0;
                for (ObjectProvider op : opsForTable)
                {
                    assertReadOnlyForUpdateOfObject(op);
                    AbstractClassMetaData cmd = op.getClassMetaData();
                    if (!storeMgr.managesClass(cmd.getFullClassName()))
                    {
                        // Make sure schema exists (using this connection)
                        ((MongoDBStoreManager)storeMgr).addClasses(new String[] {cmd.getFullClassName()}, op.getExecutionContext().getClassLoaderResolver(), db);
                    }

                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.Start", 
                            op.getObjectAsPrintable(), op.getInternalObjectId()));
                    }

                    dbObjects[i] = getDBObjectForObjectProviderToInsert(op, true);

                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.ObjectPersisted",
                            op.getObjectAsPrintable(), op.getInternalObjectId()));
                    }

                    i++;
                }

                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Persisting objects as " + StringUtils.objectArrayToString(dbObjects));
                }
                collection.insert(dbObjects, new WriteConcern(1));
                if (ec.getStatistics() != null)
                {
                    ec.getStatistics().incrementNumWrites();
                    for (int j=0;j<dbObjects.length;j++)
                    {
                        ec.getStatistics().incrementInsertCount();
                    }
                }

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }
            }
            catch (MongoException me)
            {
                NucleusLogger.PERSISTENCE.error("Exception inserting objects", me);
                throw new NucleusDataStoreException("Exception inserting objects", me);
            }
            finally
            {
                mconn.release();
            }
        }
    }

    public void insertObject(ObjectProvider op)
    {
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            AbstractClassMetaData cmd = op.getClassMetaData();
            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((MongoDBStoreManager)storeMgr).addClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), db);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            DBCollection collection = db.getCollection(storeMgr.getNamingFactory().getTableName(cmd));
            DBObject dbObject = getDBObjectForObjectProviderToInsert(op, !cmd.pkIsDatastoreAttributed(storeMgr));

            NucleusLogger.DATASTORE_NATIVE.debug("Persisting object " + op + " as " + dbObject);
            collection.insert(dbObject, new WriteConcern(1));
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
            }

            if (cmd.pkIsDatastoreAttributed(storeMgr))
            {
                // Set the identity of the object based on the datastore-generated IDENTITY strategy value
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    // Set identity from MongoDB "_id" field
                    ObjectId idKey = (ObjectId) dbObject.get("_id");
                    op.setPostStoreNewObjectId(idKey.toString());
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.ObjectPersistedWithIdentity",
                            op.getObjectAsPrintable(), idKey));
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNumbers = cmd.getPKMemberPositions();
                    for (int i=0;i<pkFieldNumbers.length;i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                        if (storeMgr.isStrategyDatastoreAttributed(cmd, pkFieldNumbers[i]))
                        {
                            if (mmd.getType() != String.class)
                            {
                                // Field type must be String since MongoDB "_id" is a hex String.
                                throw new NucleusUserException("Any field using IDENTITY value generation with MongoDB should be of type String");
                            }
                            ObjectId idKey = (ObjectId)dbObject.get("_id");
                            op.replaceField(mmd.getAbsoluteFieldNumber(), idKey.toString());
                            op.setPostStoreNewObjectId(idKey); // TODO This is incorrect if part of a composite PK
                            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                            {
                                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.ObjectPersistedWithIdentity",
                                    op.getObjectAsPrintable(), idKey));
                            }
                        }
                    }
                }

                // Update any relation fields
                StoreFieldManager fieldManager = new StoreFieldManager(op, dbObject, true);
                int[] fieldNumbers = cmd.getRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
                if (fieldNumbers != null && fieldNumbers.length > 0)
                {
                    op.provideFields(fieldNumbers, fieldManager);
                    NucleusLogger.DATASTORE_NATIVE.debug("Saving object " + op + " as " + dbObject);
                    collection.save(dbObject);
                    if (ec.getStatistics() != null)
                    {
                        ec.getStatistics().incrementNumWrites();
                    }
                }
            }
            else
            {
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.ObjectPersisted",
                        op.getObjectAsPrintable(), op.getInternalObjectId()));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (MongoException me)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + op, me);
            throw new NucleusDataStoreException("Exception inserting object for " + op, me);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Convenience method to populate the DBObject for the object managed by the ObjectProvider.
     * @param op ObjectProvider
     * @return The DBObject to persist
     */
    private DBObject getDBObjectForObjectProviderToInsert(ObjectProvider op, boolean includeRelationFields)
    {
        DBObject dbObject = new BasicDBObject();
        AbstractClassMetaData cmd = op.getClassMetaData();

        if (cmd.getIdentityType() == IdentityType.DATASTORE && !storeMgr.isStrategyDatastoreAttributed(cmd, -1))
        {
            // Add surrogate datastore identity field (if using identity then just uses "_id" MongoDB special)
            String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
            OID oid = (OID) op.getInternalObjectId();
            Object key = oid.getKeyValue();
            dbObject.put(fieldName, key);
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            // Add discriminator field
            DiscriminatorMetaData discmd = cmd.getDiscriminatorMetaData();
            String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
            Object discVal = null;
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                discVal = cmd.getFullClassName();
            }
            else
            {
                discVal = discmd.getValue();
            }
            dbObject.put(fieldName, discVal);
        }

        // Add Multi-tenancy discriminator if applicable
        if (storeMgr.getStringProperty(PropertyNames.PROPERTY_TENANT_ID) != null)
        {
            if ("true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                // Don't bother with multitenancy for this class
            }
            else
            {
                String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.MULTITENANCY_COLUMN);
                dbObject.put(fieldName, storeMgr.getStringProperty(PropertyNames.PROPERTY_TENANT_ID));
            }
        }

        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                long versionNumber = 1;
                op.setTransactionalVersion(Long.valueOf(versionNumber));
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Insert.ObjectPersistedWithVersion",
                        op.getObjectAsPrintable(), op.getInternalObjectId(), "" + versionNumber));
                }

                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    Object verFieldValue = Long.valueOf(versionNumber);
                    if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                    {
                        verFieldValue = Integer.valueOf((int)versionNumber);
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                }
                else
                {
                    // Surrogate version
                    String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                    dbObject.put(fieldName, new Long(versionNumber));
                }
            }
            else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
            {
                Date date = new Date();
                Timestamp ts = new Timestamp(date.getTime());
                op.setTransactionalVersion(ts);

                if (vermd.getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), ts);
                }
                else
                {
                    // Surrogate version
                    String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                    dbObject.put(fieldName, ts);
                }
            }
        }

        StoreFieldManager fieldManager = new StoreFieldManager(op, dbObject, true);
        int[] fieldNumbers = cmd.getAllMemberPositions();
        if (!includeRelationFields)
        {
            ExecutionContext ec = op.getExecutionContext();
            fieldNumbers = cmd.getNonRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
        }
        op.provideFields(fieldNumbers, fieldManager);

        return dbObject;
    }

    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            DBCollection collection = db.getCollection(storeMgr.getNamingFactory().getTableName(cmd));
            DBObject dbObject = MongoDBUtils.getObjectForObjectProvider(collection, op, true, true);
            if (dbObject == null)
            {
                if (cmd.isVersioned())
                {
                    throw new NucleusOptimisticException("Object with id " + op.getInternalObjectId() + 
                        " and version " + op.getTransactionalVersion() + " no longer present");
                }
                else
                {
                    throw new NucleusDataStoreException("Could not find object with id " + op.getInternalObjectId());
                }
            }

            int[] updatedFieldNums = fieldNumbers;
            if (cmd.isVersioned())
            {
                // Version object so calculate version to store with
                Object currentVersion = op.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
                op.setTransactionalVersion(nextVersion);

                if (vermd.getFieldName() != null)
                {
                    // Update the version field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length+1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                    dbObject.put(fieldName, nextVersion);
                }
            }

            StoreFieldManager fieldManager = new StoreFieldManager(op, dbObject, false);
            op.provideFields(updatedFieldNums, fieldManager);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Updating object " + op + " as " + dbObject);
            }
            collection.save(dbObject);
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (MongoException me)
        {
            NucleusLogger.PERSISTENCE.error("Exception updating object " + op, me);
            throw new NucleusDataStoreException("Exception updating object for " + op, me);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#deleteObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void deleteObjects(ObjectProvider... ops)
    {
        // TODO If MongoDB java driver ever provides bulk delete of multiple objects at once, support it
        super.deleteObjects(ops);
    }

    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.Delete.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            DBCollection collection = db.getCollection(storeMgr.getNamingFactory().getTableName(cmd));
            DBObject dbObject = MongoDBUtils.getObjectForObjectProvider(collection, op, true, false);
            if (dbObject == null)
            {
                if (cmd.isVersioned())
                {
                    throw new NucleusOptimisticException("Object with id " + op.getInternalObjectId() + 
                        " and version " + op.getTransactionalVersion() + " no longer present");
                }
                else
                {
                    throw new NucleusDataStoreException("Could not find object with id " + op.getInternalObjectId());
                }
            }
            // Save the dbObject in case we need to load fields during the deletion
            op.setAssociatedValue(OP_DB_OBJECT, dbObject);

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op, true));

            // Delete this object
            op.removeAssociatedValue(OP_DB_OBJECT);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Removing object " + op + " as " + dbObject);
            }
            collection.remove(dbObject);
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_MONGODB.msg("MongoDB.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (MongoException me)
        {
            NucleusLogger.PERSISTENCE.error("Exception deleting object " + op, me);
            throw new NucleusDataStoreException("Exception deleting object for " + op, me);
        }
        finally
        {
            mconn.release();
        }
    }

    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(op.getObjectAsPrintable()).append("\" (id=");
                str.append(op.getInternalObjectId()).append(")").append(" fields [");
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.DATASTORE_RETRIEVE.debug(str.toString());
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_MONGODB.msg("MongoDB.Fetch.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            DBObject dbObject = (DBObject) op.getAssociatedValue(OP_DB_OBJECT);
            if (dbObject == null)
            {
                DBCollection collection = db.getCollection(storeMgr.getNamingFactory().getTableName(cmd));
                dbObject = MongoDBUtils.getObjectForObjectProvider(collection, op, false, false);
                if (dbObject == null)
                {
                    throw new NucleusObjectNotFoundException("Could not find object with id " + op.getInternalObjectId() + " op="+op);
                }
            }

            FetchFieldManager fieldManager = new FetchFieldManager(op, dbObject, cmd);
            op.replaceFields(fieldNumbers, fieldManager);

            if (cmd.isVersioned() && op.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it (note we do this after the retrieval of fields in case just got version)
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion =
                        op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                    op.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                    Object datastoreVersion = dbObject.get(fieldName);
                    op.setVersion(datastoreVersion);
                }
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_MONGODB.msg("MongoDB.ExecutionTime",
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public Object findObject(ExecutionContext om, Object id)
    {
        return null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#locateObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void locateObjects(ObjectProvider[] ops)
    {
        // TODO Implement bulk location of objects. Split into DBCollections, then do bulk get
        super.locateObjects(ops);
    }

    public void locateObject(ObjectProvider op)
    {
        final AbstractClassMetaData cmd = op.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || 
            cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                DB db = (DB)mconn.getConnection();
                DBCollection collection = db.getCollection(storeMgr.getNamingFactory().getTableName(cmd));
                DBObject dbObject = MongoDBUtils.getObjectForObjectProvider(collection, op, false, false);
                if (dbObject == null)
                {
                    throw new NucleusObjectNotFoundException();
                }
            }
            finally
            {
                mconn.release();
            }
        }
    }
}