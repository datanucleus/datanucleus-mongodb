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
package org.datanucleus.store.mongodb.fieldmanager;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.mongodb.DBObject;
import com.mongodb.DBRef;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.ValueGenerationStrategy;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConversionHelper;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;

/**
 * Field Manager for retrieving values from MongoDB.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected Table table;

    protected DBObject dbObject;

    /** Metadata for the owner field if this is embedded. TODO Is this needed now that we have "mmds" in EmbeddedFetchFieldManager? */
    protected AbstractMemberMetaData ownerMmd = null;

    public FetchFieldManager(DNStateManager sm, DBObject dbObject, Table table)
    {
        super(sm);
        this.table = table;
        this.dbObject = dbObject;
    }

    public FetchFieldManager(ExecutionContext ec, DBObject dbObject, AbstractClassMetaData cmd, Table table)
    {
        super(ec, cmd);
        this.table = table;
        this.dbObject = dbObject;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Boolean.valueOf(dflt);
            }
            return false;
        }

        Object value = dbObject.get(fieldName);
        return ((Boolean)value).booleanValue();
    }

    @Override
    public byte fetchByteField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Byte.valueOf(dflt);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((Number)value).byteValue();
    }

    @Override
    public char fetchCharField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null && dflt.length() > 0)
            {
                return dflt.charAt(0);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((String)value).charAt(0);
    }

    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Double.valueOf(dflt);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((Number)value).doubleValue();
    }

    @Override
    public float fetchFloatField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Float.valueOf(dflt);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((Number)value).floatValue();
    }

    @Override
    public int fetchIntField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Integer.valueOf(dflt);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((Number)value).intValue();
    }

    @Override
    public long fetchLongField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Long.valueOf(dflt);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((Number)value).longValue();
    }

    @Override
    public short fetchShortField(int fieldNumber)
    {
        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (!dbObject.containsField(fieldName))
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            String dflt = MongoDBUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Short.valueOf(dflt);
            }
            return 0;
        }

        Object value = dbObject.get(fieldName);
        return ((Number)value).shortValue();
    }

    @Override
    public String fetchStringField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getValueStrategy() == ValueGenerationStrategy.IDENTITY)
        {
            // Return the "_id" value since not using this field as such
            Object id = dbObject.get("_id");
            return id.toString();
        }

        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        if (mapping.getTypeConverter() != null)
        {
            // Fetch using the provided converter
            String colName = mapping.getColumn(0).getName();
            if (!dbObject.containsField(colName))
            {
                return null;
            }
            return (String) mapping.getTypeConverter().toMemberType(dbObject.get(colName));
        }

        String fieldName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (dbObject.containsField(fieldName))
        {
            return (String)(dbObject.get(fieldName));
        }

        return null;
    }

    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
        {
            return sm != null ? sm.provideField(fieldNumber) : null;
        }

        StoreManager storeMgr = ec.getStoreManager();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, ownerMmd))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object
                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (embcmd == null)
                {
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
                }

                if (MongoDBUtils.isMemberNested(mmd))
                {
                    // Nested embedding, as nested document
                    if (ownerMmd != null)
                    {
                        if (RelationType.isBidirectional(relationType))
                        {
                            if ((ownerMmd.getMappedBy() != null && mmd.getName().equals(ownerMmd.getMappedBy())) ||
                                    (mmd.getMappedBy() != null && ownerMmd.getName().equals(mmd.getMappedBy())))
                            {
                                // Other side of owner bidirectional, so return the owner
                                DNStateManager ownerSM = ec.getOwnerForEmbeddedStateManager(sm);
                                return (ownerSM != null) ? ownerSM.getObject() : null;
                            }
                        }
                        else
                        {
                            // mapped-by not set but could have owner-field
                            if (ownerMmd.hasCollection())
                            {
                                if (ownerMmd.getElementMetaData().getEmbeddedMetaData() != null &&
                                        ownerMmd.getElementMetaData().getEmbeddedMetaData().getOwnerMember() != null &&
                                        ownerMmd.getElementMetaData().getEmbeddedMetaData().getOwnerMember().equals(mmd.getName()))
                                {
                                    // This is the owner-field linking back to the owning object so return the owner
                                    DNStateManager ownerSM = ec.getOwnerForEmbeddedStateManager(sm);
                                    return (ownerSM != null) ? ownerSM.getObject() : null;
                                }
                            }
                            else if (ownerMmd.getEmbeddedMetaData() != null &&
                                    ownerMmd.getEmbeddedMetaData().getOwnerMember() != null &&
                                    ownerMmd.getEmbeddedMetaData().getOwnerMember().equals(mmd.getName()))
                            {
                                // This is the owner-field linking back to the owning object so return the owner
                                DNStateManager ownerSM = ec.getOwnerForEmbeddedStateManager(sm);
                                return (ownerSM != null) ? ownerSM.getObject() : null;
                            }
                        }
                    }

                    MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                    String fieldName = mapping.getColumn(0).getName();
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }

                    DBObject embeddedValue = (DBObject)dbObject.get(fieldName);
                    if (embcmd.hasDiscriminatorStrategy())
                    {
                        // Set embcmd based on the discriminator value of this element
                        String discPropName = null;
                        if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getDiscriminatorMetaData() != null)
                        {
                            discPropName = mmd.getEmbeddedMetaData().getDiscriminatorMetaData().getColumnName();
                        }
                        else
                        {
                            discPropName = storeMgr.getNamingFactory().getColumnName(embcmd, ColumnType.DISCRIMINATOR_COLUMN); // TODO Use Table
                        }
                        String discVal = (String)embeddedValue.get(discPropName);
                        String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, embcmd.getDiscriminatorMetaData());
                        if (!elemClassName.equals(embcmd.getFullClassName()))
                        {
                            embcmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                        }
                    }

                    List<AbstractMemberMetaData> embMmds = new ArrayList<>();
                    embMmds.add(mmd);

                    DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embcmd, sm, fieldNumber, PersistableObjectType.EMBEDDED_PC);
                    FetchFieldManager ffm = new FetchEmbeddedFieldManager(embSM, embeddedValue, embMmds, table);
                    embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                    return embSM.getObject();
                }

                // Flat embedding as fields of the owning document
                if (embcmd.hasDiscriminatorStrategy())
                {
                    // Set embcmd based on the discriminator value of this element
                    String discPropName = null;
                    if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getDiscriminatorMetaData() != null)
                    {
                        discPropName = mmd.getEmbeddedMetaData().getDiscriminatorMetaData().getColumnName();
                    }
                    else
                    {
                        discPropName = storeMgr.getNamingFactory().getColumnName(embcmd, ColumnType.DISCRIMINATOR_COLUMN); // TODO Use Table
                    }
                    String discVal = (String)dbObject.get(discPropName);
                    String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, embcmd.getDiscriminatorMetaData());
                    if (!elemClassName.equals(embcmd.getFullClassName()))
                    {
                        embcmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                    }
                }

                // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)
                EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
                boolean isNull = true;
                int i = 0;
                // TODO This is utterly wrong. We got the embmmds above but then ignore them in the loop. Use the embmmd
                List<AbstractMemberMetaData> embmmds = embmd.getMemberMetaData();
                Iterator<AbstractMemberMetaData> embMmdsIter = embmmds.iterator();
                while (embMmdsIter.hasNext())
                {
                    embMmdsIter.next(); // TODO Not using this!!
                    String embFieldName = MongoDBUtils.getFieldName(mmd, i);
                    if (dbObject.containsField(embFieldName))
                    {
                        isNull = false;
                        break;
                    }
                    i++;
                }
                if (isNull)
                {
                    return null;
                }

                List<AbstractMemberMetaData> embMmds = new ArrayList<>();
                embMmds.add(mmd);
                DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embcmd, sm, fieldNumber, PersistableObjectType.EMBEDDED_PC);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, dbObject, embMmds, table);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                if (mmd.hasCollection())
                {
                    // Embedded Collection<PC>, stored nested
                    MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                    String fieldName = mapping.getColumn(0).getName();
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }
                    Object value = dbObject.get(fieldName);
                    Collection<Object> coll;
                    AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    Collection collValue = (Collection)value;
                    Iterator collIter = collValue.iterator();
                    while (collIter.hasNext())
                    {
                        DBObject elementObj = (DBObject)collIter.next();
                        AbstractClassMetaData elementCmd = elemCmd;
                        if (elementCmd.hasDiscriminatorStrategy())
                        {
                            // Set elementCmd based on the discriminator value of this element
                            String discPropName = null;
                            if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getDiscriminatorMetaData() != null)
                            {
                                discPropName = mmd.getEmbeddedMetaData().getDiscriminatorMetaData().getColumnName();
                            }
                            else
                            {
                                discPropName = storeMgr.getNamingFactory().getColumnName(elementCmd, ColumnType.DISCRIMINATOR_COLUMN); // TODO Use Table
                            }
                            String discVal = (String)elementObj.get(discPropName);
                            String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, elementCmd.getDiscriminatorMetaData());
                            if (!elemClassName.equals(elementCmd.getFullClassName()))
                            {
                                elementCmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                            }
                        }

                        DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elementCmd, sm, fieldNumber, 
                            PersistableObjectType.EMBEDDED_COLLECTION_ELEMENT_PC);

                        String embClassName = embSM.getClassMetaData().getFullClassName();
                        StoreData sd = storeMgr.getStoreDataForClass(embClassName);
                        if (sd == null)
                        {
                            storeMgr.manageClasses(clr, embClassName);
                            sd = storeMgr.getStoreDataForClass(embClassName);
                        }
                        Table elemTable = sd.getTable();
                        // TODO Use FetchEmbeddedFieldManager
                        FetchFieldManager ffm = new FetchFieldManager(embSM, elementObj, elemTable);
                        ffm.ownerMmd = mmd;
                        embSM.replaceFields(elementCmd.getAllMemberPositions(), ffm);
                        coll.add(embSM.getObject());
                    }

                    if (sm != null)
                    {
                        return SCOUtils.wrapSCOField(sm, fieldNumber, coll, true);
                    }
                    return coll;
                }
                else if (mmd.hasArray())
                {
                    // Embedded [PC], stored nested
                    MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                    String fieldName = mapping.getColumn(0).getName();
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }

                    AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr);
                    Object value = dbObject.get(fieldName);
                    Object[] array = new Object[Array.getLength(value)];
                    for (int i=0;i<array.length;i++)
                    {
                        DBObject elementObj = (DBObject)Array.get(value, i);
                        AbstractClassMetaData elementCmd = elemCmd;
                        if (elementCmd.hasDiscriminatorStrategy())
                        {
                            // Set elementCmd based on the discriminator value of this element
                            String discPropName = null;
                            if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getDiscriminatorMetaData() != null)
                            {
                                discPropName = mmd.getEmbeddedMetaData().getDiscriminatorMetaData().getColumnName();
                            }
                            else
                            {
                                discPropName = storeMgr.getNamingFactory().getColumnName(elementCmd, ColumnType.DISCRIMINATOR_COLUMN); // TODO Use Table
                            }
                            String discVal = (String)elementObj.get(discPropName);
                            String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, elementCmd.getDiscriminatorMetaData());
                            if (!elemClassName.equals(elementCmd.getFullClassName()))
                            {
                                elementCmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                            }
                        }

                        DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, elementCmd, sm, fieldNumber,
                            PersistableObjectType.EMBEDDED_ARRAY_ELEMENT_PC);

                        String embClassName = embSM.getClassMetaData().getFullClassName();
                        StoreData sd = storeMgr.getStoreDataForClass(embClassName);
                        if (sd == null)
                        {
                            storeMgr.manageClasses(clr, embClassName);
                            sd = storeMgr.getStoreDataForClass(embClassName);
                        }
                        Table elemTable = sd.getTable();
                        // TODO Use FetchEmbeddedFieldManager
                        FetchFieldManager ffm = new FetchFieldManager(embSM, elementObj, elemTable);
                        ffm.ownerMmd = mmd;
                        embSM.replaceFields(elementCmd.getAllMemberPositions(), ffm);
                        array[i] = embSM.getObject();
                    }

                    return array;
                }
                else
                {
                    // Embedded Map<NonPC,PC>, Map<PC,NonPC>, Map<PC,PC>, stored nested
                    MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                    String fieldName = mapping.getColumn(0).getName();
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }
                    Object value = dbObject.get(fieldName);

                    Map map = null;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
                        map = (Map) instanceType.getDeclaredConstructor().newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
                    AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr);
                    Collection<DBObject> entryColl = (Collection)value;
                    for (DBObject entryObj : entryColl)
                    {
                        Object keyObj = entryObj.get("key");
                        Object valObj = entryObj.get("value");

                        Object mapKey = null;
                        if (keyCmd != null)
                        {
                            // Key is embedded object
                            DBObject keyDbObj = (DBObject)keyObj;

                            AbstractClassMetaData theKeyCmd = keyCmd;
                            if (theKeyCmd.hasDiscriminatorStrategy())
                            {
                                // Set elementCmd based on the discriminator value of this element
                                String discPropName = null;
                                if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getDiscriminatorMetaData() != null)
                                {
                                    discPropName = mmd.getEmbeddedMetaData().getDiscriminatorMetaData().getColumnName();
                                }
                                else
                                {
                                    discPropName = storeMgr.getNamingFactory().getColumnName(theKeyCmd, ColumnType.DISCRIMINATOR_COLUMN); // TODO Use Table
                                }
                                String discVal = (String)keyDbObj.get(discPropName);
                                String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, theKeyCmd.getDiscriminatorMetaData());
                                if (!elemClassName.equals(theKeyCmd.getFullClassName()))
                                {
                                    theKeyCmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                                }
                            }

                            DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, theKeyCmd, sm, fieldNumber,
                                PersistableObjectType.EMBEDDED_MAP_KEY_PC);

                            String embClassName = embSM.getClassMetaData().getFullClassName();
                            StoreData sd = storeMgr.getStoreDataForClass(embClassName);
                            if (sd == null)
                            {
                                storeMgr.manageClasses(clr, embClassName);
                                sd = storeMgr.getStoreDataForClass(embClassName);
                            }
                            Table keyTable = sd.getTable();
                            // TODO Use FetchEmbeddedFieldManager
                            FetchFieldManager ffm = new FetchFieldManager(embSM, keyDbObj, keyTable);
                            ffm.ownerMmd = mmd;
                            embSM.replaceFields(theKeyCmd.getAllMemberPositions(), ffm);
                            mapKey = embSM.getObject();
                        }
                        else
                        {
                            mapKey = getMapKeyForReturnValue(mmd, keyObj);
                        }

                        Object mapVal = null;
                        if (valCmd != null)
                        {
                            // Value is embedded object
                            DBObject valDbObj = (DBObject)valObj;

                            AbstractClassMetaData theValCmd = valCmd;
                            if (theValCmd.hasDiscriminatorStrategy())
                            {
                                // Set elementCmd based on the discriminator value of this element
                                String discPropName = null;
                                if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getDiscriminatorMetaData() != null)
                                {
                                    discPropName = mmd.getEmbeddedMetaData().getDiscriminatorMetaData().getColumnName();
                                }
                                else
                                {
                                    discPropName = storeMgr.getNamingFactory().getColumnName(theValCmd, ColumnType.DISCRIMINATOR_COLUMN); // TODO Use Table
                                }
                                String discVal = (String)valDbObj.get(discPropName);
                                String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, theValCmd.getDiscriminatorMetaData());
                                if (!elemClassName.equals(theValCmd.getFullClassName()))
                                {
                                    theValCmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                                }
                            }

                            DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, theValCmd, sm, fieldNumber,
                                PersistableObjectType.EMBEDDED_MAP_VALUE_PC);

                            String embClassName = embSM.getClassMetaData().getFullClassName();
                            StoreData sd = storeMgr.getStoreDataForClass(embClassName);
                            if (sd == null)
                            {
                                storeMgr.manageClasses(clr, embClassName);
                                sd = storeMgr.getStoreDataForClass(embClassName);
                            }
                            Table valTable = sd.getTable();
                            // TODO Use FetchEmbeddedFieldManager
                            FetchFieldManager ffm = new FetchFieldManager(embSM, valDbObj, valTable);
                            ffm.ownerMmd = mmd;
                            embSM.replaceFields(theValCmd.getAllMemberPositions(), ffm);
                            mapVal = embSM.getObject();
                        }
                        else
                        {
                            mapVal = getMapValueForReturnValue(mmd, valObj);
                        }

                        map.put(mapKey, mapVal);
                    }

                    if (sm != null)
                    {
                        return SCOUtils.wrapSCOField(sm, fieldNumber, map, true);
                    }
                    return map;
                }
            }
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        boolean optional = false;
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }
            optional = true;
        }

        if (mapping.getNumberOfColumns() == 1 && !dbObject.containsField(mapping.getColumn(0).getName()))
        {
            return optional ? Optional.empty() : null;
        }

        Object value = dbObject.get(mapping.getColumn(0).getName());
        if (mmd.isSerialized())
        {
            Object obj = MongoDBUtils.getFieldValueForJavaSerialisedField(mmd, value);
            if (sm != null)
            {
                // Wrap if SCO
                obj = SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), obj, true);
            }

            if (RelationType.isRelationSingleValued(relationType))
            {
                // Make sure this object is managed by an StateManager
                DNStateManager embSM = ec.findStateManager(obj);
                if (embSM == null || ec.getApiAdapter().getExecutionContext(obj) == null)
                {
                    ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, obj, false, sm, fieldNumber, PersistableObjectType.EMBEDDED_PC);
                }
            }
            return obj;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            Object memberValue = getValueForSingleRelationField(mmd, value, clr);
            return optional ? Optional.of(memberValue) : memberValue;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            return getValueForContainerRelationField(mmd, value, clr);
        }

        Object val = null;
        if (mapping.getTypeConverter() != null)
        {
            TypeConverter conv = mapping.getTypeConverter();
            if (mapping.getNumberOfColumns() > 1)
            {
                boolean isNull = true;
                Object valuesArr = null;
                Class[] colTypes = ((MultiColumnConverter)conv).getDatastoreColumnTypes();
                if (colTypes[0] == int.class)
                {
                    valuesArr = new int[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == long.class)
                {
                    valuesArr = new long[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == double.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == float.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == String.class)
                {
                    valuesArr = new String[mapping.getNumberOfColumns()];
                }
                // TODO Support other types
                else
                {
                    valuesArr = new Object[mapping.getNumberOfColumns()];
                }

                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    String colName = mapping.getColumn(i).getName();
                    if (dbObject.containsField(colName))
                    {
                        isNull = false;
                        Array.set(valuesArr, i, dbObject.get(colName));
                    }
                    else
                    {
                        Array.set(valuesArr, i, null);
                    }
                }

                if (isNull)
                {
                    return null;
                }

                val = conv.toMemberType(valuesArr);
            }
            else
            {
                String colName = mapping.getColumn(0).getName();
                if (!dbObject.containsField(colName))
                {
                    return null;
                }
                Object propVal = dbObject.get(colName);
                Class datastoreType = ec.getTypeManager().getDatastoreTypeForTypeConverter(conv, mmd.getType());
                if (!datastoreType.isAssignableFrom(propVal.getClass()))
                {
                    // Need to do conversion to the correct type for the converter e.g datastore returned java.util.Date yet need java.sql.Timestamp/Date/Time
                    if (datastoreType == java.sql.Timestamp.class || datastoreType == java.sql.Date.class || datastoreType == java.sql.Time.class)
                    {
                        propVal = TypeConversionHelper.convertTo(propVal, datastoreType);
                    }
                    else
                    {
                        NucleusLogger.PERSISTENCE.warn("Retrieved value for member " + mmd.getFullFieldName() + " needs to be converted to member type " + mmd.getTypeName() +
                            " yet the converter needs type=" + datastoreType.getName() + " and datastore returned " + propVal.getClass().getName());
                    }
                }
                val = conv.toMemberType(propVal);
            }
        }
        else
        {
            val = MongoDBUtils.getFieldValueFromStored(ec, mmd, mapping, value, FieldRole.ROLE_FIELD);
        }
        val = optional ? Optional.of(val) : val;

        return (sm!=null) ? SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), val, true) : val;
    }

    protected Object getValueForSingleRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        if (value instanceof DBRef)
        {
            // TODO Cater for DBRef
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a DBRef stored in it. We do not currently support DBRef links. See the DataNucleus documentation");
        }

        String idStr = (String)value;
        if (value == null)
        {
            return null;
        }

        Class memberType = mmd.getType();
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            memberType = clr.classForName(mmd.getCollection().getElementType());
        }
        try
        {
            AbstractClassMetaData memberCmd = ec.getMetaDataManager().getMetaDataForClass(memberType, clr);
            if (memberCmd != null)
            {
                // Persistable field
                if (memberCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                {
                    // Uses persistent identity
                    return IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
                }

                // Uses legacy identity
                return IdentityUtils.getObjectFromIdString(idStr, memberCmd, ec, true);
            }

            // Interface field
            String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_FIELD, clr, ec.getMetaDataManager());
            if (implNames != null && implNames.length == 1)
            {
                // Only one possible implementation, so use that
                memberCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                return IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
            }
            else if (implNames != null && implNames.length > 1)
            {
                // Multiple implementations, so try each implementation in turn (note we only need this if some impls have different "identity" type from each other)
                for (String implName : implNames)
                {
                    try
                    {
                        memberCmd = ec.getMetaDataManager().getMetaDataForClass(implName, clr);
                        return IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
                    }
                    catch (NucleusObjectNotFoundException nonfe)
                    {
                        // Object no longer present in the datastore, must have been deleted
                        throw nonfe;
                    }
                    catch (Exception e)
                    {
                        // Not possible with this implementation
                    }
                }
            }

            throw new NucleusUserException(
                "We do not currently support the field type of " + mmd.getFullFieldName() + " which has an interdeterminate type (e.g interface or Object element types)");
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            NucleusLogger.GENERAL.warn("Object=" + sm + " field=" + mmd.getFullFieldName() + " has id=" + idStr + " but could not instantiate object with that identity");
            return null;
        }
    }

    protected Object getValueForContainerRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        if (mmd.hasCollection())
        {
            // "a,b,c,d,..."
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, FieldRole.ROLE_COLLECTION_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length > 0)
                {
                    // Just use first implementation TODO What if the impls have different id type?
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() + 
                        " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            // TODO Support DBRef option rather than persistable id
            Collection collIds = (Collection)value;
            Iterator idIter = collIds.iterator();
            boolean changeDetected = false;
            AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(ec.getClassLoaderResolver());
            while (idIter.hasNext())
            {
                Object idValue = idIter.next();
                if (idValue instanceof DBRef)
                {
                    // TODO Cater for DBRef
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a DBRef stored in it. We do not currently support DBRef links. See the DataNucleus documentation");
                }

                String elementIdStr = (String)idValue;
                if (elementIdStr.equals("NULL"))
                {
                    coll.add(null);
                }
                else
                {
                    try
                    {
                        Object element = null;
                        if (elementCmd.usesSingleFieldIdentityClass() && elementIdStr.indexOf(':') > 0)
                        {
                            // Uses persistent identity
                            element = IdentityUtils.getObjectFromPersistableIdentity(elementIdStr, elementCmd, ec);
                        }
                        else
                        {
                            // Uses legacy identity
                            element = IdentityUtils.getObjectFromIdString(elementIdStr, elementCmd, ec, true);
                        }
                        coll.add(element);
                    }
                    catch (NucleusObjectNotFoundException onfe)
                    {
                        // Object no longer exists. Deleted by user? so ignore
                        changeDetected = true;
                    }
                }
            }

            if (coll instanceof List && mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null && !mmd.getOrderMetaData().getOrdering().equals("#PK"))
            {
                // Reorder the collection as per the ordering clause
                Collection newColl = QueryUtils.orderCandidates((List)coll, clr.classForName(mmd.getCollection().getElementType()), mmd.getOrderMetaData().getOrdering(), ec, clr);
                if (newColl.getClass() != coll.getClass())
                {
                    // Type has changed, so just reuse the input
                    coll.clear();
                    coll.addAll(newColl);
                }
            }

            if (sm != null)
            {
                // Wrap if SCO
                coll = (Collection) SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), coll, true);
                if (changeDetected)
                {
                    sm.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return coll;
        }
        else if (mmd.hasMap())
        {
            // "List<Entry>" where the entry object has "key", "value" fields
            Map map = null;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
                map = (Map) instanceType.getDeclaredConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            // TODO Support DBRef option rather than persistable id
            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
            AbstractClassMetaData valueCmd = mmd.getMap().getValueClassMetaData(clr);
            Collection<DBObject> collEntries = (Collection)value;
            boolean changeDetected = false;
            for (DBObject entryObj : collEntries)
            {
                Object keyObj = entryObj.get("key");
                Object valueObj = entryObj.get("value");

                if (keyObj instanceof DBRef)
                {
                    // TODO Cater for DBRef
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a key with DBRef stored in it. We do not currently support DBRef links. See the DataNucleus documentation");
                }

                try
                {
                    Object mapKey = null;
                    if (keyCmd != null)
                    {
                        // TODO handle Map<interface, ?>
                        String keyStr = (String)keyObj;
                        if (keyCmd.usesSingleFieldIdentityClass() && keyStr.indexOf(':') > 0)
                        {
                            // Uses persistent identity
                            mapKey = IdentityUtils.getObjectFromPersistableIdentity(keyStr, keyCmd, ec);
                        }
                        else
                        {
                            // Uses legacy identity
                            mapKey = IdentityUtils.getObjectFromIdString(keyStr, keyCmd, ec, true);
                        }
                    }
                    else
                    {
                        mapKey = getMapKeyForReturnValue(mmd, keyObj);
                    }

                    if (valueObj instanceof DBRef)
                    {
                        // TODO Cater for DBRef
                        throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a value with DBRef stored in it. We do not currently support DBRef links. See the DataNucleus documentation");
                    }

                    Object mapVal = null;
                    if (valueCmd != null)
                    {
                        // TODO handle Collection<?, interface>
                        String valStr = (String)valueObj;
                        if (valueCmd.usesSingleFieldIdentityClass() && valStr.indexOf(':') > 0)
                        {
                            // Uses persistent identity
                            mapVal = IdentityUtils.getObjectFromPersistableIdentity(valStr, valueCmd, ec);
                        }
                        else
                        {
                            // Uses legacy identity
                            mapVal = IdentityUtils.getObjectFromIdString(valStr, valueCmd, ec, true);
                        }
                    }
                    else
                    {
                        mapVal = getMapValueForReturnValue(mmd, valueObj);
                    }

                    map.put(mapKey, mapVal);
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // Object no longer exists. Deleted by user? so ignore
                    changeDetected = true;
                }
            }

            if (sm != null)
            {
                // Wrap if SCO
                map = (Map)SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), map, true);
                if (changeDetected)
                {
                    sm.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return map;
        }
        else if (mmd.hasArray())
        {
            // "a,b,c,d,..."
            AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr);
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                    FieldRole.ROLE_ARRAY_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has an array of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            Collection collIds = (Collection)value;
            Iterator idIter = collIds.iterator();
            int i = 0;
            Object array = Array.newInstance(mmd.getType().getComponentType(), collIds.size());
            boolean changeDetected = false;
            while (idIter.hasNext())
            {
                Object idValue = idIter.next();

                // TODO handle interface[]
                if (idValue instanceof DBRef)
                {
                    // TODO Cater for DBRef
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has an array with DBRef stored in it. We do not currently support DBRef links. See the DataNucleus documentation");
                }

                String elementIdStr = (String)idValue;
                if (elementIdStr.equals("NULL"))
                {
                    Array.set(array, i++, null);
                }
                else
                {
                    try
                    {
                        Object element = null;
                        if (elemCmd.usesSingleFieldIdentityClass() && elementIdStr.indexOf(':') > 0)
                        {
                            // Uses persistent identity
                            element = IdentityUtils.getObjectFromPersistableIdentity(elementIdStr, elemCmd, ec);
                        }
                        else
                        {
                            // Uses legacy identity
                            element = IdentityUtils.getObjectFromIdString(elementIdStr, elemCmd, ec, true);
                        }
                        Array.set(array, i++, element);
                    }
                    catch (NucleusObjectNotFoundException onfe)
                    {
                        // Object no longer exists. Deleted by user? so ignore
                        changeDetected = true;
                    }
                }
            }

            if (changeDetected)
            {
                if (i < collIds.size())
                {
                    // Some elements not found, so resize the array
                    Object arrayOld = array;
                    array = Array.newInstance(mmd.getType().getComponentType(), i);
                    for (int j=0;j<i;j++)
                    {
                        Array.set(array, j, Array.get(arrayOld, j));
                    }
                }
                if (sm != null)
                {
                    sm.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return array;
        }
        else
        {
            return value;
        }
    }

    private Object getMapKeyForReturnValue(AbstractMemberMetaData mmd, Object value)
    {
        String keyType = mmd.getMap().getKeyType();
        Class keyCls = ec.getClassLoaderResolver().classForName(keyType);
        if (keyCls == Long.class || keyCls == Double.class || keyCls == Float.class || keyCls == Integer.class || keyCls == Short.class || keyCls == String.class)
        {
            return value;
        }
        else if (Enum.class.isAssignableFrom(keyCls))
        {
            return Enum.valueOf(keyCls, (String)value);
        }
        else
        {
            throw new NucleusException("Dont currently support persistence/retrieval of maps with keys of type " + keyType);
        }
    }

    private Object getMapValueForReturnValue(AbstractMemberMetaData mmd, Object value)
    {
        String valueType = mmd.getMap().getValueType();
        Class valueCls = ec.getClassLoaderResolver().classForName(valueType);
        if (valueCls == Long.class || valueCls == Double.class || valueCls == Float.class || valueCls == Integer.class || valueCls == Short.class || valueCls == String.class)
        {
            return value;
        }
        else if (Enum.class.isAssignableFrom(valueCls))
        {
            return Enum.valueOf(valueCls, (String)value);
        }
        else
        {
            throw new NucleusException("Dont currently support persistence/retrieval of maps with values of type " + valueType);
        }
    }
}
