/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
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
2012 Chris Rued - patch for MapReduceCount and collection.count()
   ...
**********************************************************************/
package org.datanucleus.store.mongodb;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.mongodb.fieldmanager.FetchFieldManager;
import org.datanucleus.store.mongodb.query.LazyLoadQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;

import java.util.LinkedList;

import javax.imageio.ImageIO;

/**
 * Utilities for MongoDB.
 */
public class MongoDBUtils
{
    public static List<Long> performMongoCount(DB db, BasicDBObject filterObject, Class candidateClass,
        boolean subclasses, ExecutionContext ec)
    throws MongoException 
    {
        StoreManager storeMgr = ec.getStoreManager();
        long count = 0;
        for (AbstractClassMetaData cmd : MetaDataUtils.getMetaDataForCandidates(candidateClass, subclasses, ec)) 
        {
            String collectionName = storeMgr.getNamingFactory().getTableName(cmd);
            count += db.getCollection(collectionName).count(filterObject);
        }
        List<Long> results =  new LinkedList<Long>();
        results.add(count);
        if (ec.getStatistics() != null)
        {
            // Add to statistics
            ec.getStatistics().incrementNumReads();
        }
        return results;
    }

    /**
     * Accessor for the default value specified for the provided member.
     * If no defaultValue is provided on the column then returns null.
     * @param mmd Metadata for the member
     * @return The default value
     */
    public static String getDefaultValueForMember(AbstractMemberMetaData mmd)
    {
        ColumnMetaData[] colmds = mmd.getColumnMetaData();
        if (colmds == null || colmds.length < 1)
        {
            return null;
        }
        return colmds[0].getDefaultValue();
    }

    /**
     * Accessor for the MongoDB field for the field of this embedded field.
     * Uses the column name from the embedded definition (if present), otherwise falls back to
     * the column name of the field in its own class definition, otherwise uses its field name.
     * @param mmd Metadata for the owning member
     * @param fieldNumber Member number of the embedded object
     * @return The field name to use
     */
    public static String getFieldName(AbstractMemberMetaData mmd, int fieldNumber)
    {
        String columnName = null;

        // Try the first column if specified
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        AbstractMemberMetaData embMmd = null;
        if (embmd != null)
        {
            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
            embMmd = embmmds[fieldNumber];
        }

        if (embMmd != null)
        {
            ColumnMetaData[] colmds = embMmd.getColumnMetaData();
            if (colmds != null && colmds.length > 0)
            {
                columnName = colmds[0].getName();
            }
            if (columnName == null)
            {
                // Fallback to the field/property name
                columnName = embMmd.getName();
            }
            if (columnName == null)
            {
                columnName = embMmd.getName();
            }
        }
        return columnName;
    }

    /**
     * Convenience method that tries to find the object with the specified identity from all DBCollection objects
     * from the rootCmd and subclasses. Returns the class name of the object with this identity (or null if not found).
     * @param id The identity
     * @param rootCmd ClassMetaData for the root class in the inheritance tree
     * @param ec ExecutionContext
     * @param clr ClassLoader resolver
     * @return The class name of the object with this id
     */
    public static String getClassNameForIdentity(Object id, AbstractClassMetaData rootCmd, ExecutionContext ec,
            ClassLoaderResolver clr)
    {
        Map<String, Set<String>> classNamesByDbCollectionName = new HashMap<String, Set<String>>();
        StoreManager storeMgr = ec.getStoreManager();
        Set rootClassNames = new HashSet<String>();
        rootClassNames.add(rootCmd.getFullClassName());
        classNamesByDbCollectionName.put(storeMgr.getNamingFactory().getTableName(rootCmd), rootClassNames);
        String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(rootCmd.getFullClassName(), true);
        if (subclassNames != null)
        {
            for (int i=0;i<subclassNames.length;i++)
            {
                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(subclassNames[i], clr);
                String collName = storeMgr.getNamingFactory().getTableName(cmd);
                Set<String> classNames = classNamesByDbCollectionName.get(collName);
                if (classNames == null)
                {
                    classNames = new HashSet<String>();
                    classNamesByDbCollectionName.put(collName, classNames);
                }
                classNames.add(cmd.getFullClassName());
            }
        }

        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            for (Map.Entry<String, Set<String>> dbCollEntry : classNamesByDbCollectionName.entrySet())
            {
                // Check each DBCollection for the id PK field(s)
                String dbCollName = dbCollEntry.getKey();
                Set<String> classNames = dbCollEntry.getValue();
                DBCollection dbColl = db.getCollection(dbCollName);
                BasicDBObject query = new BasicDBObject();
                if (id instanceof OID)
                {
                    // TODO Really ought to use cmd of the class using this dbCollection
                    Object key = ((OID)id).getKeyValue();
                    if (storeMgr.isStrategyDatastoreAttributed(rootCmd, -1))
                    {
                        query.put("_id", new ObjectId((String)key));
                    }
                    else
                    {
                        query.put(storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.DATASTOREID_COLUMN), key);
                    }
                }
                else if (ec.getApiAdapter().isSingleFieldIdentity(id))
                {
                    Object key = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(id);
                    int[] pkNums = rootCmd.getPKMemberPositions();
                    AbstractMemberMetaData pkMmd = rootCmd.getMetaDataForManagedMemberAtAbsolutePosition(pkNums[0]);
                    String pkPropName = storeMgr.getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN);
                    query.put(pkPropName, key);
                }
                else
                {
                    int[] pkNums = rootCmd.getPKMemberPositions();
                    for (int i=0;i<pkNums.length;i++)
                    {
                        AbstractMemberMetaData pkMmd = rootCmd.getMetaDataForManagedMemberAtAbsolutePosition(pkNums[i]);
                        String pkPropName = storeMgr.getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN);
                        Object pkVal = IdentityUtils.getValueForMemberInId(id, pkMmd);
                        query.put(pkPropName, pkVal);
                    }
                }

                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Retrieving object for " + query + " from DBCollection with name " + dbCollName);
                }
                DBObject foundObj = dbColl.findOne(query);
                if (ec.getStatistics() != null)
                {
                    // Add to statistics
                    ec.getStatistics().incrementNumReads();
                }

                if (foundObj != null)
                {
                    if (classNames.size() == 1)
                    {
                        // Only one candidate so return it
                        return classNames.iterator().next();
                    }
                    else
                    {
                        if (rootCmd.hasDiscriminatorStrategy())
                        {
                            String disPropName = storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.DISCRIMINATOR_COLUMN);
                            String discValue = (String)foundObj.get(disPropName);
                            return ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discValue, rootCmd.getDiscriminatorMetaData());
                        }
                        else
                        {
                            // Fallback to the root class since no discriminator
                            return rootCmd.getFullClassName();
                        }
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }
        return null;
    }

    /**
     * Method to return the DBObject that equates to the provided object.
     * @param dbCollection The collection in which it is stored
     * @param op The ObjectProvider
     * @param checkVersion Whether to also check for a particular version
     * @param originalValue Whether to use the original value of fields (when using nondurable id and doing update).
     * @return The object (or null if not found)
     */
    public static DBObject getObjectForObjectProvider(DBCollection dbCollection, ObjectProvider op, boolean checkVersion,
            boolean originalValue)
    {
        // Build query object to use as template for the find
        BasicDBObject query = new BasicDBObject();
        AbstractClassMetaData cmd = op.getClassMetaData();
        StoreManager storeMgr = op.getExecutionContext().getStoreManager();

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Application id - Add PK field(s) to the query object
            int[] pkPositions = cmd.getPKMemberPositions();
            for (int i=0;i<pkPositions.length;i++)
            {
                AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
                Object value = op.provideField(pkPositions[i]);
                if (value == null && storeMgr.isStrategyDatastoreAttributed(cmd, pkPositions[i]))
                {
                    // PK field not yet set, so return null (needs to be attributed in the datastore)
                    return null;
                }
                if (storeMgr.isStrategyDatastoreAttributed(cmd, pkPositions[i]))
                {
                    query.put("_id", new ObjectId((String)value));
                }
                else
                {
                    Object storeValue = MongoDBUtils.getStoredValueForField(op.getExecutionContext(), pkMmd, value, FieldRole.ROLE_FIELD);
                    query.put(storeMgr.getNamingFactory().getColumnName(pkMmd, ColumnType.COLUMN), storeValue);
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Datastore id - Add "id" field to the query object
            OID oid = (OID) op.getInternalObjectId();
            if (oid == null && storeMgr.isStrategyDatastoreAttributed(cmd, -1))
            {
                // Not yet set, so return null (needs to be attributed in the datastore)
                return null;
            }
            Object value = oid.getKeyValue();
            if (storeMgr.isStrategyDatastoreAttributed(cmd, -1))
            {
                query.put("_id", new ObjectId((String)value));
            }
            else
            {
                query.put(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN), value);
            }
        }
        else
        {
            // Nondurable - Add all basic field(s) to the query object
            int[] fieldNumbers = cmd.getAllMemberPositions();
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(op.getExecutionContext().getClassLoaderResolver());
                if (relationType == RelationType.NONE)
                {
                    Object fieldValue = null;
                    if (originalValue)
                    {
                        Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumbers[i]);
                        if (oldValue != null)
                        {
                            fieldValue = oldValue;
                        }
                        else
                        {
                            fieldValue = op.provideField(fieldNumbers[i]);
                        }
                    }
                    else
                    {
                        fieldValue = op.provideField(fieldNumbers[i]);
                    }

                    Object storeValue = MongoDBUtils.getStoredValueForField(op.getExecutionContext(), mmd, fieldValue, FieldRole.ROLE_FIELD);
                    query.put(storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), storeValue);
                }
            }
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            // Add discriminator to the query object
            query.put(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN), cmd.getDiscriminatorValue());
        }
        if (checkVersion && cmd.isVersioned())
        {
            // Add version to the query object
            Object currentVersion = op.getTransactionalVersion();
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Version field in class
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                String fieldName = storeMgr.getNamingFactory().getColumnName(verMmd, ColumnType.COLUMN);
                query.put(fieldName, currentVersion);
            }
            else
            {
                // Surrogate version field
                String fieldName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                query.put(fieldName, currentVersion);
            }
        }

        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug("Retrieving object for " + query);
        }
        DBObject dbObj = dbCollection.findOne(query);
        if (op.getExecutionContext().getStatistics() != null)
        {
            // Add to statistics
            op.getExecutionContext().getStatistics().incrementNumReads();
        }
        return dbObj;
    }

    public static List getObjectsOfCandidateType(Query q, DB db, BasicDBObject filterObject,
            Map<String, Object> options)
    {
        return getObjectsOfCandidateType(q, db, filterObject, null, options, null, null);
    }

    /**
     * Convenience method to return all objects of the candidate type (optionally allowing subclasses).
     * @param q Query
     * @param db Mongo DB
     * @param filterObject Optional filter object
     * @param orderingObject Optional ordering object
     * @param options Set of options for controlling this query
     * @param skip Number of records to skip
     * @param limit Max number of records to return
     * @return List of all candidate objects (implements QueryResult)
     */
    public static List getObjectsOfCandidateType(Query q, DB db, BasicDBObject filterObject, BasicDBObject orderingObject,
            Map<String, Object> options, Integer skip, Integer limit)
    {
        LazyLoadQueryResult qr = new LazyLoadQueryResult(q);

        // Find the DBCollections we need to query
        ExecutionContext ec = q.getExecutionContext();
        StoreManager storeMgr = ec.getStoreManager();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        List<AbstractClassMetaData> cmds =
            MetaDataUtils.getMetaDataForCandidates(q.getCandidateClass(), q.isSubclasses(), ec);

        Map<String, List<AbstractClassMetaData>> classesByCollectionName = new HashMap();
        for (AbstractClassMetaData cmd : cmds)
        {
            if (cmd instanceof ClassMetaData && ((ClassMetaData)cmd).isAbstract())
            {
                // Omit any classes that are not instantiable (e.g abstract)
            }
            else
            {
                String collectionName = storeMgr.getNamingFactory().getTableName(cmd);
                List<AbstractClassMetaData> cmdsForCollection = classesByCollectionName.get(collectionName);
                if (cmdsForCollection == null)
                {
                    cmdsForCollection = new ArrayList();
                    classesByCollectionName.put(collectionName, cmdsForCollection);
                }
                cmdsForCollection.add(cmd);
            }
        }

        // Add a query for each DBCollection we need
        Iterator<Map.Entry<String, List<AbstractClassMetaData>>> iter = classesByCollectionName.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, List<AbstractClassMetaData>> entry = iter.next();
            String collectionName = entry.getKey();
            List<AbstractClassMetaData> cmdsForCollection = entry.getValue();

            AbstractClassMetaData rootCmd = cmdsForCollection.get(0);
            int[] fpMembers = q.getFetchPlan().getFetchPlanForClass(rootCmd).getMemberNumbers();
            BasicDBObject fieldsSelection = new BasicDBObject();
            if (fpMembers != null && fpMembers.length > 0)
            {
                fieldsSelection = new BasicDBObject();
                for (int i=0;i<fpMembers.length;i++)
                {
                    AbstractMemberMetaData mmd = rootCmd.getMetaDataForManagedMemberAtAbsolutePosition(fpMembers[i]);
                    RelationType relationType = mmd.getRelationType(clr);
                    if (mmd.isEmbedded() && RelationType.isRelationSingleValued(relationType))
                    {
                        boolean nested = true;
                        String nestedStr = mmd.getValueForExtension("nested");
                        if (nestedStr != null && nestedStr.equalsIgnoreCase("false"))
                        {
                            nested = false;
                        }

                        if (nested)
                        {
                            // Nested Embedded field, so include field
                            String fieldName = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                            fieldsSelection.append(fieldName, 1);
                        }
                        else
                        {
                            // Flat Embedded field, so add all fields of sub-objects
                            selectAllFieldsOfEmbeddedObject(mmd, fieldsSelection, ec, clr);
                        }
                    }
                    else
                    {
                        String fieldName = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                        fieldsSelection.append(fieldName, 1);
                    }
                }
            }
            if (rootCmd.getIdentityType() == IdentityType.DATASTORE)
            {
                fieldsSelection.append(storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.DATASTOREID_COLUMN), 1);
            }
            if (rootCmd.isVersioned())
            {
                VersionMetaData vermd = rootCmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    AbstractMemberMetaData verMmd = rootCmd.getMetaDataForMember(vermd.getFieldName());
                    String fieldName = storeMgr.getNamingFactory().getColumnName(verMmd, ColumnType.COLUMN);
                    fieldsSelection.append(fieldName, 1);
                }
                else
                {
                    fieldsSelection.append(storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.VERSION_COLUMN), 1);
                }
            }
            if (rootCmd.hasDiscriminatorStrategy())
            {
                fieldsSelection.append(storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.DISCRIMINATOR_COLUMN), 1);
            }

            BasicDBObject query = new BasicDBObject();
            if (filterObject != null)
            {
                Iterator<Map.Entry<String, Object>> filterEntryIter = filterObject.entrySet().iterator();
                while (filterEntryIter.hasNext())
                {
                    Map.Entry<String, Object> filterEntry = filterEntryIter.next();
                    query.put(filterEntry.getKey(), filterEntry.getValue());
                }
            }

            if (rootCmd.hasDiscriminatorStrategy() && cmdsForCollection.size() == 1)
            {
                // TODO Add this restriction on *all* possible cmds for this DBCollection
                // Discriminator present : Add restriction on the discriminator value for this class
                query.put(storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.DISCRIMINATOR_COLUMN),
                    rootCmd.getDiscriminatorValue());
            }

            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null)
            {
                // Multitenancy discriminator present : Add restriction for this tenant
                if ("true".equalsIgnoreCase(rootCmd.getValueForExtension("multitenancy-disable")))
                {
                    // Don't bother with multitenancy for this class
                }
                else
                {
                    String fieldName = storeMgr.getNamingFactory().getColumnName(rootCmd, ColumnType.MULTITENANCY_COLUMN);
                    String value = storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID);
                    query.put(fieldName, value);
                }
            }

            DBCollection dbColl = db.getCollection(collectionName);
            Object val = (options != null ? options.get("slave-ok") : Boolean.FALSE);
            if (val == Boolean.TRUE)
            {
                dbColl.setReadPreference(ReadPreference.secondaryPreferred());
            }

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Performing find() using query on collection " + collectionName +
                    " for fields=" + fieldsSelection + " with filter=" + query + " and ordering=" + orderingObject);
            }
            DBCursor curs = dbColl.find(query, fieldsSelection);
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }

            if (classesByCollectionName.size() == 1)
            {
                if (orderingObject != null)
                {
                    curs = curs.sort(orderingObject);
                    qr.setOrderProcessed(true);
                }

                // We have a single DBCursor so apply the range specification directly to this DBCursor
                if (skip != null && skip > 0)
                {
                    curs = curs.skip(skip);
                    qr.setRangeProcessed(true);
                }
                if (limit != null && limit > 0)
                {
                    curs = curs.limit(limit);
                    qr.setRangeProcessed(true);
                }
            }

            if (curs.hasNext())
            {
                // Contains result(s) so add it to our QueryResult
                qr.addCandidateResult(rootCmd, curs, fpMembers);
            }
        }

        return qr;
    }

    /**
     * Convenience method that takes the provided DBObject and the details of the candidate that it is an instance
     * of, and converts it into the associated POJO.
     * @param dbObject The DBObject
     * @param ec ExecutionContext
     * @param cmd Metadata for the candidate class
     * @param fpMembers FetchPlan members for the class that are provided
     * @param ignoreCache Whether to ignore the cache
     * @return The Pojo object
     */
    public static Object getPojoForDBObjectForCandidate(DBObject dbObject, ExecutionContext ec,
            AbstractClassMetaData cmd, int[] fpMembers, boolean ignoreCache)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Determine the class from the discriminator property
            String disPropName = ec.getStoreManager().getNamingFactory().getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN);
            String discValue = (String)dbObject.get(disPropName);
            String clsName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discValue, cmd.getDiscriminatorMetaData());
            if (!cmd.getFullClassName().equals(clsName) && clsName != null)
            {
                cmd = ec.getMetaDataManager().getMetaDataForClass(clsName, ec.getClassLoaderResolver());
            }
        }

        Object pojo = null;
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            pojo = MongoDBUtils.getObjectUsingApplicationIdForDBObject(dbObject, cmd, ec, ignoreCache, fpMembers);
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            pojo = MongoDBUtils.getObjectUsingDatastoreIdForDBObject(dbObject, cmd, ec, ignoreCache, fpMembers);
        }
        else
        {
            pojo = MongoDBUtils.getObjectUsingNondurableIdForDBObject(dbObject, cmd, ec, ignoreCache, fpMembers);
        }
        return pojo;
    }

    protected static void selectAllFieldsOfEmbeddedObject(AbstractMemberMetaData mmd, BasicDBObject fieldsSelection,
            ExecutionContext ec, ClassLoaderResolver clr)
    {
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
        for (int i=0;i<embmmds.length;i++)
        {
            RelationType relationType = embmmds[i].getRelationType(clr);
            if (embmmds[i].isEmbedded() && RelationType.isRelationSingleValued(relationType))
            {
                selectAllFieldsOfEmbeddedObject(embmmds[i], fieldsSelection, ec, clr);
            }
            else
            {
                String embFieldName = MongoDBUtils.getFieldName(mmd, i);
                fieldsSelection.append(embFieldName, 1);
            }
        }
    }

    public static Object getObjectUsingApplicationIdForDBObject(final DBObject dbObject, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        final FetchFieldManager fm = new FetchFieldManager(ec, dbObject, cmd);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        StoreManager storeMgr = ec.getStoreManager();
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, 
            new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, type, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = dbObject.get(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN));
            }
            sm.setVersion(version);
        }
        return pc;
    }

    public static Object getObjectUsingDatastoreIdForDBObject(final DBObject dbObject, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        Object idKey = null;
        StoreManager storeMgr = ec.getStoreManager();
        if (storeMgr.isStrategyDatastoreAttributed(cmd, -1))
        {
            idKey = dbObject.get("_id");
            if (idKey instanceof ObjectId)
            {
                idKey = ((ObjectId)idKey).toString();
            }
        }
        else
        {
            idKey = dbObject.get(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
        }

        final FetchFieldManager fm = new FetchFieldManager(ec, dbObject, cmd);
        OID oid = OIDFactory.getInstance(ec.getNucleusContext(), cmd.getFullClassName(), idKey);
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(oid, 
            new FieldValues()
            {
                // ObjectProvider calls the fetchFields method
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, type, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = dbObject.get(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN));
            }
            sm.setVersion(version);
        }
        return pc;
    }
    public static Object getObjectUsingNondurableIdForDBObject(final DBObject dbObject, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        SCOID oid = new SCOID(cmd.getFullClassName());
        final FetchFieldManager fm = new FetchFieldManager(ec, dbObject, cmd);
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(oid, 
            new FieldValues()
            {
                // ObjectProvider calls the fetchFields method
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, type, ignoreCache, false);

        if (cmd.isVersioned())
        {
            // Set the version on the retrieved object
            ObjectProvider sm = ec.findObjectProvider(pc);
            StoreManager storeMgr = ec.getStoreManager();
            Object version = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Get the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = dbObject.get(storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN));
            }
            sm.setVersion(version);
        }
        return pc;
    }

    /**
     * Convenience method to convert the raw value of an object field into the value that will be stored
     * in MongoDB. Note that this does not cater for relation fields, just basic fields.
     * @param ec ExecutionContext
     * @param mmd Metadata for the field holding this value (if available)
     * @param value The raw value for the field
     * @param fieldRole The role of this value for the field
     * @return The value to store
     */
    public static Object getStoredValueForField(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, int fieldRole)
    {
        if (value == null)
        {
            return null;
        }

        Class type = value.getClass();
        if (mmd != null)
        {
            if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_KEY)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
            }
            else
            {
                type = mmd.getType();
            }
        }

        if (mmd != null && mmd.hasCollection() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection coll = new ArrayList();
            Collection rawColl = (Collection)value;
            for (Object elem : rawColl)
            {
                Object storeElem = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
                coll.add(storeElem);
            }
            return coll;
        }
        else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Object[] array = new Object[Array.getLength(value)];
            for (int i=0;i<array.length;i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_ARRAY_ELEMENT);
                array[i] = storeElem;
            }
            return array;
        }

        if (Long.class.isAssignableFrom(type) ||
            Integer.class.isAssignableFrom(type) ||
            Short.class.isAssignableFrom(type) ||
            String.class.isAssignableFrom(type) ||
            Byte.class.isAssignableFrom(type) ||
            Boolean.class.isAssignableFrom(type))
        {
            return value;
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            boolean useNumeric = MetaDataUtils.persistColumnAsNumeric(colmd);
            return useNumeric ? ((Enum)value).ordinal() : ((Enum)value).name();
        }
        else if (java.sql.Time.class.isAssignableFrom(type))
        {
            // Will be processed by type converters below
        }
        else if (java.sql.Date.class.isAssignableFrom(type))
        {
            // Will be processed by type converters below
        }
        else if (Date.class.isAssignableFrom(type))
        {
            // store as-is
            return value;
        }
        else if (Calendar.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            if (!MetaDataUtils.persistColumnAsString(colmd))
            {
                // Persisted as Date
                return ((Calendar)value).getTime();
            }
        }
        else if (Character.class.isAssignableFrom(type))
        {
            // store as String
            return "" + value;
        }
        else if (char.class.isAssignableFrom(type))
        {
            // store as String
            return "" + value;
        }
        else if (BufferedImage.class.isAssignableFrom(type))
        {
            // store serialised
            byte[] bytes = null;
            try
            {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                ImageIO.write((BufferedImage) value, "jpg", baos);
                bytes = baos.toByteArray();
                baos.close();
            }
            catch (IOException e)
            {
            }
            return bytes;
        }

        // Fallback to built-in type converters
        // TODO Make use of default TypeConverter for a type before falling back to String/Long
        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (strConv != null)
        {
            // store as a String
            return strConv.toDatastoreType(value);
        }
        else if (longConv != null)
        {
            // store as a Long
            return longConv.toDatastoreType(value);
        }

        // store as the raw value
        return value;
    }

    /**
     * Convenience method to convert the stored value for an object field into the value that will be held
     * in the object. Note that this does not cater for relation fields, just basic fields.
     * @param ec ExecutionContext
     * @param mmd Metadata for the field holding this value (if available)
     * @param value The stored value for the field
     * @param fieldRole The role of this value for the field
     * @return The value to put in the field
     */
    public static Object getFieldValueFromStored(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, int fieldRole)
    {
        if (value == null)
        {
            return null;
        }

        Class type = value.getClass();
        if (mmd != null)
        {
            if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_KEY)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
            }
            else
            {
                type = mmd.getType();
            }
        }

        if (mmd != null && mmd.hasCollection() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            Collection rawColl = (Collection)value;
            for (Object elem : rawColl)
            {
                Object storeElem = getFieldValueFromStored(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
                coll.add(storeElem);
            }
            return coll;
        }
        else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection rawColl = (Collection)value;
            Object array = Array.newInstance(mmd.getType().getComponentType(), rawColl.size());
            int i=0;
            for (Object elem : rawColl)
            {
                Object storeElem = getFieldValueFromStored(ec, mmd, elem, FieldRole.ROLE_ARRAY_ELEMENT);
                Array.set(array, i++, storeElem);
            }
            return array;
        }

        if (Character.class.isAssignableFrom(type))
        {
            if (value instanceof Character)
            {
                return value;
            }
            return ((String)value).charAt(0);
        }
        else if (Short.class.isAssignableFrom(type))
        {
            if (value instanceof Short)
            {
                return value;
            }
            return ((Number)value).shortValue();
        }
        else if (Integer.class.isAssignableFrom(type))
        {
            if (value instanceof Integer)
            {
                return value;
            }
            return ((Number)value).intValue();
        }
        else if (Long.class.isAssignableFrom(type))
        {
            if (value instanceof Long)
            {
                return value;
            }
            return ((Number)value).longValue();
        }
        else if (Float.class.isAssignableFrom(type))
        {
            if (value instanceof Float)
            {
                return value;
            }
            return ((Number)value).floatValue();
        }
        else if (Double.class.isAssignableFrom(type))
        {
            if (value instanceof Double)
            {
                return value;
            }
            return ((Number)value).doubleValue();
        }
        else if (Byte.class.isAssignableFrom(type))
        {
            if (value instanceof Byte)
            {
                return value;
            }
            return ((Number)value).byteValue();
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            if (MetaDataUtils.persistColumnAsNumeric(colmd))
            {
                return type.getEnumConstants()[((Number)value).intValue()];
            }
            else
            {
                return Enum.valueOf(type, (String)value);
            }
        }
        else if (java.sql.Date.class.isAssignableFrom(type) && value instanceof Date)
        {
            java.sql.Date sqlDate = null;
            if (value instanceof java.sql.Date)
            {
                sqlDate = (java.sql.Date) value;
            }
            else
            {
                sqlDate = new java.sql.Date(((Date)value).getTime());
            }
            return sqlDate;
        }
        else if (java.sql.Time.class.isAssignableFrom(type) && value instanceof Date)
        {
            java.sql.Time sqlTime = null;
            if (value instanceof java.sql.Time)
            {
                sqlTime = (java.sql.Time) value;
            }
            else
            {
                sqlTime = new java.sql.Time(((Date)value).getTime());
            }
            return sqlTime;
        }
        else if (java.sql.Timestamp.class.isAssignableFrom(type) && value instanceof Date)
        {
            java.sql.Timestamp sqlTs = null;
            if (value instanceof java.sql.Timestamp)
            {
                sqlTs = (java.sql.Timestamp) value;
            }
            else
            {
                sqlTs = new java.sql.Timestamp(((Date)value).getTime());
            }
            return sqlTs;
        }
        else if (Date.class.isAssignableFrom(type) && value instanceof Date)
        {
            // Date could have been persisted as String (in 3.0m4) so this just handles the Date case
            return value;
        }
        else if (Calendar.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            if (!MetaDataUtils.persistColumnAsString(colmd))
            {
                // Persisted as Date
                Calendar cal = Calendar.getInstance();
                cal.setTime((Date)value);
                return cal;
            }
        }
        else if (BufferedImage.class.isAssignableFrom(type))
        {
            Object returnValue = null;
            try
            {
                returnValue = ImageIO.read(new ByteArrayInputStream((byte[])value));
            }
            catch (IOException e)
            {
            }
            return returnValue;
        }

        // TODO Make use of default TypeConverter for a type before falling back to String/Long
        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (strConv != null)
        {
            // Persisted as a String, so convert back
            String strValue = (String)value;
            return strConv.toMemberType(strValue);
        }
        else if (longConv != null)
        {
            // Persisted as a Long, so convert back
            Long longValue = (Long)value;
            return longConv.toMemberType(longValue);
        }

        return value;
    }

    /**
     * Method to return the value to store when using java serialisation.
     * @param mmd Metadata of the member
     * @param value The raw value of the member
     * @return The value to store
     */
    public static byte[] getStoredValueForJavaSerialisedField(AbstractMemberMetaData mmd, Object value)
    {
        byte[] storeValue = null;
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            storeValue = bos.toByteArray();
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException("Exception thrown serialising value for field " + mmd.getFullFieldName(), e);
        }
        return storeValue;
    }

    /**
     * Method to return the raw value when stored java serialised.
     * @param mmd Metadata for the member
     * @param value The serialised value
     * @return The raw value
     */
    public static Object getFieldValueForJavaSerialisedField(AbstractMemberMetaData mmd, Object value)
    {
        Object returnValue = null;
        try
        {
            if (value != null)
            {
                byte[] bytes = (byte[])value;
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                returnValue = ois.readObject();
                ois.close();
                bis.close();
            }
        }
        catch (Exception e)
        {
            throw new NucleusUserException("Exception thrown deserialising field at " + mmd.getFullFieldName(), e);
        }
        return returnValue;
    }
}