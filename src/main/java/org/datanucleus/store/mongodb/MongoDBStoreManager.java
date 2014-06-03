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
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
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

        schemaHandler = new MongoDBSchemaHandler(this);
        persistenceHandler = new MongoDBPersistenceHandler(this);

        logConfiguration();
    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
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
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
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
    public String getStrategyForNative(AbstractClassMetaData cmd, int absFieldNumber)
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
            if (idmd != null && idmd.getColumnMetaData() != null && MetaDataUtils.isJdbcTypeString(idmd.getColumnMetaData().getJdbcType()))
            {
                return "uuid-hex";
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
     * @see org.datanucleus.store.AbstractStoreManager#manageClasses(org.datanucleus.ClassLoaderResolver, java.lang.String[])
     */
    @Override
    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = getConnection(-1);
        try
        {
            DB db = (DB)mconn.getConnection();

            manageClasses(classNames, clr, db);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, DB db)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        // TODO Change this to use schemaHandler.createSchemaForClasses in single call
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isAbstract())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
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

    public void createSchema(String schemaName, Properties props)
    {
        schemaHandler.createSchema(schemaName, props, null);
    }

    public void deleteSchema(String schemaName, Properties props)
    {
        schemaHandler.deleteSchema(schemaName, props, null);
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