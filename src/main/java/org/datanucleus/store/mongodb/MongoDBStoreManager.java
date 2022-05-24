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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.DatastoreIdentityMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.metadata.ValueGenerationStrategy;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.mongodb.query.JDOQLQuery;
import org.datanucleus.store.mongodb.query.JPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.Localiser;

import com.mongodb.DB;

public class MongoDBStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.mongodb.Localisation", MongoDBStoreManager.class.getClassLoader());
    }

    public static final String CAPPED_SIZE_EXTENSION_NAME = "mongodb.capped.size";

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public MongoDBStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext nucleusCtx, Map<String, Object> props)
    {
        super("mongodb", clr, nucleusCtx, props);

        // Default to nested embedded PC objects
        nucleusCtx.getConfiguration().setProperty(PropertyNames.PROPERTY_METADATA_EMBEDDED_PC_FLAT, "false");

        schemaHandler = new MongoDBSchemaHandler(this);
        persistenceHandler = new MongoDBPersistenceHandler(this);

        logConfiguration();
    }

    public Collection<String> getSupportedOptions()
    {
        Set<String> set = new HashSet<>();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_NONDURABLE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_COLLECTION);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_MAP);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_ARRAY);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC_NESTED);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_COLLECTION_NESTED);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_MAP_NESTED);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_ARRAY_NESTED);
        set.add(StoreManager.OPTION_ORM_SERIALISED_PC);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        set.add(StoreManager.OPTION_DATASTORE_TIME_STORES_MILLISECS);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
        set.add(StoreManager.OPTION_ORM_INHERITANCE_COMPLETE_TABLE);
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java.lang.Object,
     * org.datanucleus.ClassLoaderResolver, org.datanucleus.store.ExecutionContext)
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

        // Find overall root class possible for this id
        String rootClassName = super.getClassNameForObjectID(id, clr, ec);
        if (rootClassName != null)
        {
            // User could have passed in a superclass of the real class, so consult the datastore for the precise table/class
            String[] subclasses = getMetaDataManager().getSubclassesForClass(rootClassName, true);
            if (subclasses == null || subclasses.length == 0)
            {
                // No subclasses so no need to go to the datastore
                return rootClassName;
            }

            AbstractClassMetaData rootCmd = getMetaDataManager().getMetaDataForClass(rootClassName, clr);
            return MongoDBUtils.getClassNameForIdentity(id, rootCmd, ec, clr);
        }
        return null;
    }

    /**
     * Accessor for whether this value strategy is supported. Overrides the superclass to allow for "IDENTITY"
     * since we support it and no entry in plugins for it.
     * @param strategy The strategy
     * @return Whether it is supported.
     */
    @Override
    public boolean supportsValueGenerationStrategy(String strategy)
    {
        if (super.supportsValueGenerationStrategy(strategy))
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

    @Override
    public String getValueGenerationStrategyForNative(AbstractClassMetaData cmd)
    {
        DatastoreIdentityMetaData idmd = cmd.getBaseDatastoreIdentityMetaData();
        if (idmd != null && idmd.getColumnMetaData() != null && MetaDataUtils.isJdbcTypeString(idmd.getColumnMetaData().getJdbcType()))
        {
            return ValueGenerationStrategy.UUIDHEX.toString();
        }

        if (supportsValueGenerationStrategy(ValueGenerationStrategy.IDENTITY.toString()))
        {
            return ValueGenerationStrategy.IDENTITY.toString();
        }
        else if (supportsValueGenerationStrategy(ValueGenerationStrategy.SEQUENCE.toString()) && idmd != null && idmd.getSequence() != null)
        {
            return ValueGenerationStrategy.SEQUENCE.toString();
        }
        else if (supportsValueGenerationStrategy(ValueGenerationStrategy.INCREMENT.toString()))
        {
            return ValueGenerationStrategy.INCREMENT.toString();
        }
        throw new NucleusUserException("This datastore provider doesn't support numeric native strategy for class " + cmd.getFullClassName());
    }

    @Override
    public String getValueGenerationStrategyForNative(AbstractMemberMetaData mmd)
    {
        Class type = mmd.getType();
        if (String.class.isAssignableFrom(type))
        {
            if (supportsValueGenerationStrategy(ValueGenerationStrategy.IDENTITY.toString()))
            {
                return ValueGenerationStrategy.IDENTITY.toString();
            }

            return ValueGenerationStrategy.UUIDHEX.toString();
        }
        else if (type == Long.class || type == Integer.class || type == Short.class || type == long.class || type == int.class || type == short.class)
        {
            if (supportsValueGenerationStrategy(ValueGenerationStrategy.SEQUENCE.toString()) && mmd.getSequence() != null)
            {
                return ValueGenerationStrategy.SEQUENCE.toString();
            }
            else if (supportsValueGenerationStrategy(ValueGenerationStrategy.INCREMENT.toString()))
            {
                return ValueGenerationStrategy.INCREMENT.toString();
            }
            throw new NucleusUserException("This datastore provider doesn't support numeric native strategy for member " + mmd.getFullFieldName());
        }

        throw new NucleusUserException("This datastore provider doesn't support native strategy for field of type " + type.getName());
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#manageClasses(org.datanucleus.ClassLoaderResolver,
     * java.lang.String[])
     */
    @Override
    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null || classNames.length == 0)
        {
            return;
        }

        ManagedConnection mconn = connectionMgr.getConnection(-1);
        try
        {
            DB db = (DB) mconn.getConnection();
            manageClasses(classNames, clr, db);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, DB db)
    {
        if (classNames == null || classNames.length == 0)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData) iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isAbstract())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        // TODO Do we really need to create a CompleteClassTable when the class is embeddedOnly???
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }

                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }

        // Create schema for classes
        schemaHandler.createSchemaForClasses(clsNameSet, null, db);
    }

    public void createDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.createDatabase(catalogName, schemaName, props, null);
    }

    public void deleteDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.deleteDatabase(catalogName, schemaName, props, null);
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.createSchemaForClasses(classNames, props, null);
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.deleteSchemaForClasses(classNames, props, null);
    }

    public void validateSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.validateSchema(classNames, props, null);
    }
}