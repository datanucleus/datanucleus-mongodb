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

import java.util.HashMap;
import java.util.HashSet;
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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.mongodb.fieldmanager.FetchFieldManager;
import org.datanucleus.store.mongodb.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

public class MongoDBPersistenceHandler extends AbstractPersistenceHandler
{
    public static final String OP_DB_OBJECT = "DB_OBJECT";

    public MongoDBPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    public void close()
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void insertObjects(ObjectProvider... sms)
    {
        if (sms.length == 1)
        {
            insertObject(sms[0]);
            return;
        }

        // Process "identity" cases first in case they are referenced
        for (ObjectProvider sm : sms)
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            if (cmd.pkIsDatastoreAttributed(storeMgr))
            {
                insertObject(sm);
            }
        }

        ExecutionContext ec = sms[0].getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            // Separate the objects to be persisted into groups, for the "table" in question
            Map<String, Set<ObjectProvider>> opsByTable = new HashMap<>();
            for (ObjectProvider sm : sms)
            {
                AbstractClassMetaData cmd = sm.getClassMetaData();
                if (!cmd.pkIsDatastoreAttributed(storeMgr))
                {
                    StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                    if (sd == null)
                    {
                        // Make sure schema exists, using this connection
                        ((MongoDBStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), db);
                        sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                    }
                    Table table = sd.getTable();
                    String tableName = table.getName();
                    Set<ObjectProvider> smsForTable = opsByTable.get(tableName);
                    if (smsForTable == null)
                    {
                        smsForTable = new HashSet<>();
                        opsByTable.put(tableName, smsForTable);
                    }
                    smsForTable.add(sm);
                }
            }

            for (Map.Entry<String, Set<ObjectProvider>> smsEntry : opsByTable.entrySet())
            {
                String tableName = smsEntry.getKey();
                Set<ObjectProvider> smsForTable = smsEntry.getValue();
                try
                {
                    long startTime = System.currentTimeMillis();

                    DBCollection collection = db.getCollection(tableName);
                    DBObject[] dbObjects = new DBObject[smsForTable.size()];
                    int i=0;
                    for (ObjectProvider sm : smsForTable)
                    {
                        assertReadOnlyForUpdateOfObject(sm);

                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Insert.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                        }

                        dbObjects[i] = getDBObjectForObjectProviderToInsert(sm, true);

                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Insert.ObjectPersisted", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                        }

                        i++;
                    }

                    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_NATIVE.debug("Persisting objects using collection.insert(" + StringUtils.objectArrayToString(dbObjects) + ") into table " + tableName);
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
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.ExecutionTime", (System.currentTimeMillis() - startTime)));
                    }
                }
                catch (MongoException me)
                {
                    NucleusLogger.PERSISTENCE.error("Exception inserting objects", me);
                    throw new NucleusDataStoreException("Exception inserting objects", me);
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public void insertObject(ObjectProvider sm)
    {
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            AbstractClassMetaData cmd = sm.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                // Make sure schema exists, using this connection
                ((MongoDBStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), db);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Insert.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            DBCollection collection = db.getCollection(table.getName());
            DBObject dbObject = getDBObjectForObjectProviderToInsert(sm, !cmd.pkIsDatastoreAttributed(storeMgr));

            NucleusLogger.DATASTORE_NATIVE.debug("Persisting object " + sm + " using collection.insert(" + dbObject + ") into table=" + table.getName());
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
                    sm.setPostStoreNewObjectId(idKey.toString());
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Insert.ObjectPersistedWithIdentity", sm.getObjectAsPrintable(), idKey));
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNumbers = cmd.getPKMemberPositions();
                    for (int i=0;i<pkFieldNumbers.length;i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                        if (storeMgr.isValueGenerationStrategyDatastoreAttributed(cmd, pkFieldNumbers[i]))
                        {
                            if (mmd.getType() != String.class)
                            {
                                // Field type must be String since MongoDB "_id" is a hex String.
                                throw new NucleusUserException("Any field using IDENTITY value generation with MongoDB should be of type String");
                            }
                            ObjectId idKey = (ObjectId)dbObject.get("_id");
                            sm.replaceField(mmd.getAbsoluteFieldNumber(), idKey.toString());
                            sm.setPostStoreNewObjectId(idKey); // TODO This is incorrect if part of a composite PK
                            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                            {
                                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Insert.ObjectPersistedWithIdentity", sm.getObjectAsPrintable(), idKey));
                            }
                        }
                    }
                }

                // Update any relation fields
                StoreFieldManager fieldManager = new StoreFieldManager(sm, dbObject, true, table);
                int[] fieldNumbers = cmd.getRelationMemberPositions(ec.getClassLoaderResolver());
                if (fieldNumbers != null && fieldNumbers.length > 0)
                {
                    sm.provideFields(fieldNumbers, fieldManager);
                    NucleusLogger.DATASTORE_NATIVE.debug("Saving object " + sm + " as " + dbObject);
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
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Insert.ObjectPersisted", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (MongoException me)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + sm, me);
            throw new NucleusDataStoreException("Exception inserting object for " + sm, me);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Convenience method to populate the DBObject for the object managed by the ObjectProvider.
     * @param sm StateManager
     * @return The DBObject to persist
     */
    private DBObject getDBObjectForObjectProviderToInsert(ObjectProvider sm, boolean includeRelationFields)
    {
        DBObject dbObject = new BasicDBObject();
        AbstractClassMetaData cmd = sm.getClassMetaData();
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
        ExecutionContext ec = sm.getExecutionContext();

        if (cmd.getIdentityType() == IdentityType.DATASTORE && !storeMgr.isValueGenerationStrategyDatastoreAttributed(cmd, -1))
        {
            // Add surrogate datastore identity field (if using identity then just uses "_id" MongoDB special)
            String fieldName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
            Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
            dbObject.put(fieldName, key);
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            // Discriminator field
            dbObject.put(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR).getName(), cmd.getDiscriminatorValue());
        }

        Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
        if (multitenancyCol != null)
        {
            String tenantId = ec.getTenantId();
            if (tenantId != null)
            {
                // Multi-tenancy discriminator
                dbObject.put(multitenancyCol.getName(), ec.getTenantId());
            }
        }

        Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
        if (softDeleteCol != null)
        {
            // Soft-delete flag
            dbObject.put(softDeleteCol.getName(), Boolean.FALSE);
        }

        VersionMetaData vermd = cmd.getVersionMetaDataForClass();
        if (vermd != null)
        {
            Object versionValue = ec.getLockManager().getNextVersion(vermd, null);
            if (vermd.getFieldName() != null)
            {
                // Version is stored in a member, so update the member too
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                Object verFieldValue = Long.valueOf((Long)versionValue);
                if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                {
                    verFieldValue = Integer.valueOf(((Long)versionValue).intValue());
                }
                sm.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
            }
            else
            {
                String fieldName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName();
                dbObject.put(fieldName, versionValue);
            }
            sm.setTransactionalVersion(versionValue);
        }

        StoreFieldManager fieldManager = new StoreFieldManager(sm, dbObject, true, table);
        int[] fieldNumbers = cmd.getAllMemberPositions();
        if (!includeRelationFields)
        {
            fieldNumbers = cmd.getNonRelationMemberPositions(ec.getClassLoaderResolver());
        }
        sm.provideFields(fieldNumbers, fieldManager);

        return dbObject;
    }

    public void updateObject(ObjectProvider sm, int[] fieldNumbers)
    {
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            long startTime = System.currentTimeMillis();

            AbstractClassMetaData cmd = sm.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                // Make sure schema exists, using this connection
                ((MongoDBStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), db);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Update.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId(), fieldStr.toString()));
            }

            DBCollection collection = db.getCollection(table.getName());
            DBObject dbObject = MongoDBUtils.getObjectForObjectProvider(collection, sm, true, true);
            if (dbObject == null)
            {
                if (cmd.isVersioned())
                {
                    throw new NucleusOptimisticException("Object with id " + sm.getInternalObjectId() + " and version " + sm.getTransactionalVersion() + " no longer present");
                }
                throw new NucleusDataStoreException("Could not find object with id " + sm.getInternalObjectId());
            }

            int[] updatedFieldNums = fieldNumbers;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Version object so calculate version to store with
                Object currentVersion = sm.getTransactionalVersion();
                Object nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                sm.setTransactionalVersion(nextVersion);

                if (vermd.getFieldName() != null)
                {
                    // Update the version field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

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
                    String fieldName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName();
                    dbObject.put(fieldName, nextVersion);
                }
            }

            StoreFieldManager fieldManager = new StoreFieldManager(sm, dbObject, false, table);
            sm.provideFields(updatedFieldNums, fieldManager);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Updating object " + sm + " using collection.save(" + dbObject + ") into table=" + table.getName());
            }
            collection.save(dbObject); // TODO If we only provide fields that are set, but want to remove (null) a field, does this do it ?
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        catch (MongoException me)
        {
            NucleusLogger.PERSISTENCE.error("Exception updating object " + sm, me);
            throw new NucleusDataStoreException("Exception updating object for " + sm, me);
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
    public void deleteObjects(ObjectProvider... sms)
    {
        // TODO If MongoDB java driver ever provides bulk delete of multiple objects at once, support it
        super.deleteObjects(sms);
    }

    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.Delete.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            DBCollection collection = db.getCollection(table.getName());
            DBObject dbObject = MongoDBUtils.getObjectForObjectProvider(collection, sm, true, false);
            if (dbObject == null)
            {
                if (cmd.isVersioned())
                {
                    throw new NucleusOptimisticException("Object with id " + sm.getInternalObjectId() + 
                        " and version " + sm.getTransactionalVersion() + " no longer present");
                }

                throw new NucleusDataStoreException("Could not find object with id " + sm.getInternalObjectId());
            }
            // Save the dbObject in case we need to load fields during the deletion
            sm.setAssociatedValue(OP_DB_OBJECT, dbObject);

            // Invoke any cascade deletion
            sm.loadUnloadedFields();
            sm.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(sm, true));

            // Delete this object
            sm.removeAssociatedValue(OP_DB_OBJECT);
            Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
            if (softDeleteCol != null)
            {
                // Update the stored surrogate value
                String fieldName = softDeleteCol.getName();
                dbObject.put(fieldName, Boolean.TRUE);

                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Updating object " + sm + " using collection.save(" + dbObject + ") into table=" + table.getName());
                }
                collection.save(dbObject);
            }
            else
            {
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Removing object " + sm + " using collection.remove(" + dbObject + ") from table=" + table.getName());
                }
                collection.remove(dbObject);
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("MongoDB.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        catch (MongoException me)
        {
            NucleusLogger.PERSISTENCE.error("Exception deleting object " + sm, me);
            throw new NucleusDataStoreException("Exception deleting object for " + sm, me);
        }
        finally
        {
            mconn.release();
        }
    }

    public void fetchObject(ObjectProvider sm, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(sm.getObjectAsPrintable()).append("\" (id=");
                str.append(sm.getInternalObjectId()).append(")").append(" fields [");
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
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("MongoDB.Fetch.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
            DBObject dbObject = (DBObject) sm.getAssociatedValue(OP_DB_OBJECT);
            if (dbObject == null)
            {
                DBCollection collection = db.getCollection(table.getName());
                dbObject = MongoDBUtils.getObjectForObjectProvider(collection, sm, false, false);
                if (dbObject == null)
                {
                    throw new NucleusObjectNotFoundException("Could not find object with id " + IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()));
                }
            }

            // Strip out any non-persistent fields
            Set<Integer> nonpersistableFields = null;
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
                {
                    if (nonpersistableFields == null)
                    {
                        nonpersistableFields = new HashSet<>();
                    }
                    nonpersistableFields.add(fieldNumbers[i]);
                }
            }
            if (nonpersistableFields != null)
            {
                // Just go through motions for non-persistable fields
                for (Integer fieldNum : nonpersistableFields)
                {
                    sm.replaceField(fieldNum, sm.provideField(fieldNum));
                }
            }
            if (nonpersistableFields == null || nonpersistableFields.size() != fieldNumbers.length)
            {
                if (nonpersistableFields != null)
                {
                    // Strip out any nonpersistable fields
                    int[] persistableFieldNums = new int[fieldNumbers.length - nonpersistableFields.size()];
                    int pos = 0;
                    for (int i = 0; i < fieldNumbers.length; i++)
                    {
                        if (!nonpersistableFields.contains(fieldNumbers[i]))
                        {
                            persistableFieldNums[pos++] = fieldNumbers[i];
                        }
                    }
                    fieldNumbers = persistableFieldNums;
                }
                FetchFieldManager fieldManager = new FetchFieldManager(sm, dbObject, table);
                sm.replaceFields(fieldNumbers, fieldManager);

                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd != null && sm.getTransactionalVersion() == null)
                {
                    // No version set, so retrieve it (note we do this after the retrieval of fields in case just got version)
                    if (vermd.getFieldName() != null)
                    {
                        // Version stored in a field
                        Object datastoreVersion = sm.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                        sm.setVersion(datastoreVersion);
                    }
                    else
                    {
                        // Surrogate version
                        String fieldName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName();
                        Object datastoreVersion = dbObject.get(fieldName);
                        sm.setVersion(datastoreVersion);
                    }
                }
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("MongoDB.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
    public void locateObjects(ObjectProvider[] sms)
    {
        // TODO Implement bulk location of objects. Split into DBCollections, then do bulk get
        super.locateObjects(sms);
    }

    public void locateObject(ObjectProvider sm)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                DB db = (DB)mconn.getConnection();

                StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                if (sd == null)
                {
                    // Make sure schema exists, using this connection
                    ((MongoDBStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), db);
                    sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                }
                Table table = sd.getTable();
                DBCollection collection = db.getCollection(table.getName());
                DBObject dbObject = MongoDBUtils.getObjectForObjectProvider(collection, sm, false, false);
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