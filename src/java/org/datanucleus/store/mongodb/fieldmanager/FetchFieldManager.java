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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.mongodb.DBObject;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;

/**
 * Field Manager for retrieving values from MongoDB.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected DBObject dbObject;

    boolean embedded = false;

    /** Metadata for the owner field if this is embedded. */
    protected AbstractMemberMetaData ownerMmd = null;

    public FetchFieldManager(ObjectProvider op, DBObject dbObject)
    {
        super(op);
        this.dbObject = dbObject;
        if (op.getEmbeddedOwners() != null)
        {
            embedded = true;
        }
    }

    public FetchFieldManager(ExecutionContext ec, DBObject dbObject, AbstractClassMetaData cmd)
    {
        super(ec, cmd);
        this.dbObject = dbObject;
    }

    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        String fieldName = getFieldName(fieldNumber);
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
        if (mmd.getValueStrategy() == IdentityStrategy.IDENTITY)
        {
            // Return the "_id" value since not using this field as such
            Object id = dbObject.get("_id");
            return (String)(id.toString());
        }
        String fieldName = getFieldName(fieldNumber);
        if (dbObject.containsField(fieldName))
        {
            return (String)(dbObject.get(fieldName));
        }
        else
        {
            return null;
        }
    }

    protected String getFieldName(int fieldNumber)
    {
        return ec.getStoreManager().getNamingFactory().getColumnName(
            cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber), ColumnType.COLUMN);
    }

    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
        {
            return op.provideField(fieldNumber);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        // Determine if this field is stored embedded
        boolean embedded = isMemberEmbedded(mmd, ownerMmd, relationType);
        if (embedded)
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object
                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (embcmd == null)
                {
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                        " marked as embedded but no such metadata");
                }

                boolean nested = true;
                String nestedStr = mmd.getValueForExtension("nested");
                if (nestedStr != null && nestedStr.equalsIgnoreCase("false"))
                {
                    nested = false;
                }

                if (nested)
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
                                ObjectProvider[] ownerOps = op.getEmbeddedOwners();
                                return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
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
                                    ObjectProvider[] ownerOps = op.getEmbeddedOwners();
                                    return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
                                }
                            }
                            else if (ownerMmd.getEmbeddedMetaData() != null &&
                                ownerMmd.getEmbeddedMetaData().getOwnerMember() != null &&
                                ownerMmd.getEmbeddedMetaData().getOwnerMember().equals(mmd.getName()))
                            {
                                // This is the owner-field linking back to the owning object so return the owner
                                ObjectProvider[] ownerOps = op.getEmbeddedOwners();
                                return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
                            }
                        }
                    }

                    String fieldName = ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
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
                            discPropName = ec.getStoreManager().getNamingFactory().getColumnName(embcmd, ColumnType.DISCRIMINATOR_COLUMN);
                        }
                        String discVal = (String)embeddedValue.get(discPropName);
                        String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, embcmd.getDiscriminatorMetaData());
                        if (!elemClassName.equals(embcmd.getFullClassName()))
                        {
                            embcmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                        }
                    }

                    ObjectProvider embOP = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                    FetchFieldManager ffm = new FetchFieldManager(embOP, embeddedValue);
                    ffm.ownerMmd = mmd;
                    ffm.embedded = true;
                    embOP.replaceFields(embcmd.getAllMemberPositions(), ffm);
                    return embOP.getObject();
                }
                else
                {
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
                            discPropName = ec.getStoreManager().getNamingFactory().getColumnName(embcmd, ColumnType.DISCRIMINATOR_COLUMN);
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
                    AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
                    boolean isNull = true;
                    for (int i=0;i<embmmds.length;i++)
                    {
                        String embFieldName = MongoDBUtils.getFieldName(mmd, i);
                        if (dbObject.containsField(embFieldName))
                        {
                            isNull = false;
                            break;
                        }
                    }
                    if (isNull)
                    {
                        return null;
                    }

                    ObjectProvider embOP = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                    FieldManager ffm = new FetchEmbeddedFieldManager(embOP, dbObject, mmd);
                    embOP.replaceFields(embcmd.getAllMemberPositions(), ffm);
                    return embOP.getObject();
                }
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                if (mmd.hasCollection())
                {
                    String fieldName = ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }
                    Object value = dbObject.get(fieldName);
                    Collection<Object> coll;
                    AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.newInstance();
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
                                discPropName = ec.getStoreManager().getNamingFactory().getColumnName(elementCmd, ColumnType.DISCRIMINATOR_COLUMN);
                            }
                            String discVal = (String)elementObj.get(discPropName);
                            String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, elementCmd.getDiscriminatorMetaData());
                            if (!elemClassName.equals(elementCmd.getFullClassName()))
                            {
                                elementCmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                            }
                        }

                        ObjectProvider embOP = ec.newObjectProviderForEmbedded(elementCmd, op, fieldNumber);
                        embOP.setPcObjectType(ObjectProvider.EMBEDDED_COLLECTION_ELEMENT_PC);
                        FetchFieldManager ffm = new FetchFieldManager(embOP, elementObj);
                        ffm.ownerMmd = mmd;
                        ffm.embedded = true;
                        embOP.replaceFields(elementCmd.getAllMemberPositions(), ffm);
                        coll.add(embOP.getObject());
                    }

                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, coll, false, false, true);
                    }
                    return coll;
                }
                else if (mmd.hasArray())
                {
                    String fieldName = ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }

                    AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
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
                                discPropName = ec.getStoreManager().getNamingFactory().getColumnName(elementCmd, ColumnType.DISCRIMINATOR_COLUMN);
                            }
                            String discVal = (String)elementObj.get(discPropName);
                            String elemClassName = ec.getMetaDataManager().getClassNameFromDiscriminatorValue(discVal, elementCmd.getDiscriminatorMetaData());
                            if (!elemClassName.equals(elementCmd.getFullClassName()))
                            {
                                elementCmd = ec.getMetaDataManager().getMetaDataForClass(elemClassName, clr);
                            }
                        }

                        ObjectProvider embOP = ec.newObjectProviderForEmbedded(elementCmd, op, fieldNumber);
                        embOP.setPcObjectType(ObjectProvider.EMBEDDED_COLLECTION_ELEMENT_PC);
                        FetchFieldManager ffm = new FetchFieldManager(embOP, elementObj);
                        ffm.ownerMmd = mmd;
                        ffm.embedded = true;
                        embOP.replaceFields(elementCmd.getAllMemberPositions(), ffm);
                        array[i] = embOP.getObject();
                    }

                    return array;
                }
                else
                {
                    // TODO Allow for inherited keys/values and discriminator
                    String fieldName = ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                    if (!dbObject.containsField(fieldName))
                    {
                        return null;
                    }
                    Object value = dbObject.get(fieldName);
                    Map map = null;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
                        map = (Map) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                    AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());
                    Collection<DBObject> entryColl = (Collection)value;
                    Iterator<DBObject> entryIter = entryColl.iterator();
                    while (entryIter.hasNext())
                    {
                        DBObject entryObj = entryIter.next();
                        Object keyObj = entryObj.get("key");
                        Object valObj = entryObj.get("value");

                        Object mapKey = null;
                        if (keyCmd != null)
                        {
                            // Key is embedded object
                            DBObject keyDbObj = (DBObject)keyObj;
                            ObjectProvider embOP = ec.newObjectProviderForEmbedded(keyCmd, op, fieldNumber);
                            embOP.setPcObjectType(ObjectProvider.EMBEDDED_MAP_KEY_PC);
                            FetchFieldManager ffm = new FetchFieldManager(embOP, keyDbObj);
                            ffm.ownerMmd = mmd;
                            ffm.embedded = true;
                            embOP.replaceFields(keyCmd.getAllMemberPositions(), ffm);
                            mapKey = embOP.getObject();
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
                            ObjectProvider embOP = ec.newObjectProviderForEmbedded(valCmd, op, fieldNumber);
                            embOP.setPcObjectType(ObjectProvider.EMBEDDED_MAP_VALUE_PC);
                            FetchFieldManager ffm = new FetchFieldManager(embOP, valDbObj);
                            ffm.ownerMmd = mmd;
                            ffm.embedded = true;
                            embOP.replaceFields(valCmd.getAllMemberPositions(), ffm);
                            mapVal = embOP.getObject();
                        }
                        else
                        {
                            mapVal = getMapValueForReturnValue(mmd, valObj);
                        }

                        map.put(mapKey, mapVal);
                    }

                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, map, false, false, true);
                    }
                    return map;
                }
            }
        }

        String fieldName = ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        if (!dbObject.containsField(fieldName))
        {
            return null;
        }

        Object value = dbObject.get(fieldName);
        if (mmd.isSerialized())
        {
            // TODO Allow other types of serialisation
            Object obj = MongoDBUtils.getFieldValueForJavaSerialisedField(mmd, value);
            if (op != null)
            {
                // Wrap if SCO
                obj = op.wrapSCOField(mmd.getAbsoluteFieldNumber(), obj, false, false, true);
            }

            if (RelationType.isRelationSingleValued(relationType))
            {
                // Make sure this object is managed by an ObjectProvider
                ObjectProvider embSM = ec.findObjectProvider(obj);
                if (embSM == null || ec.getApiAdapter().getExecutionContext(obj) == null)
                {
                    ec.newObjectProviderForEmbedded(obj, false, op, fieldNumber);
                }
            }
            return obj;
        }
        if (RelationType.isRelationSingleValued(relationType))
        {
            return getValueForSingleRelationField(mmd, value, clr);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            return getValueForContainerRelationField(mmd, value, clr);
        }
        else
        {
            ColumnMetaData colmd = null;
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            Object val = getValueForContainerNonRelationField(mmd, value, colmd);
            if (op != null)
            {
                // Wrap if SCO
                return op.wrapSCOField(mmd.getAbsoluteFieldNumber(), val, false, false, true);
            }
            return val;
        }
    }

    protected Object getValueForSingleRelationField(AbstractMemberMetaData mmd, Object value, ClassLoaderResolver clr)
    {
        String idStr = (String)value;
        if (value == null)
        {
            return null;
        }

        try
        {
            Object obj = null;
            AbstractClassMetaData memberCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            if (memberCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
            {
                // Uses persistent identity
                obj = IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
            }
            else
            {
                // Uses legacy identity
                obj = IdentityUtils.getObjectFromIdString(idStr, memberCmd, ec, true);
            }
            return obj;
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            NucleusLogger.GENERAL.warn("Object=" + op + " field=" + mmd.getFullFieldName() + " has id=" + idStr +
                " but could not instantiate object with that identity");
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
                coll = (Collection<Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                    FieldRole.ROLE_COLLECTION_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                }
            }
            Collection collIds = (Collection)value;
            Iterator idIter = collIds.iterator();
            boolean changeDetected = false;
            AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                ec.getClassLoaderResolver(), ec.getMetaDataManager());
            while (idIter.hasNext())
            {
                String elementIdStr = (String)idIter.next();
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

            if (op != null)
            {
                // Wrap if SCO
                coll = (Collection) op.wrapSCOField(mmd.getAbsoluteFieldNumber(), coll, false, false, true);
                if (changeDetected)
                {
                    op.makeDirty(mmd.getAbsoluteFieldNumber());
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
                map = (Map) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
            AbstractClassMetaData valueCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());
            Collection<DBObject> collEntries = (Collection)value;
            Iterator<DBObject> entryIter = collEntries.iterator();
            boolean changeDetected = false;
            while (entryIter.hasNext())
            {
                DBObject entryObj = entryIter.next();
                Object keyObj = entryObj.get("key");
                Object valueObj = entryObj.get("value");

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

            if (op != null)
            {
                // Wrap if SCO
                map = (Map)op.wrapSCOField(mmd.getAbsoluteFieldNumber(), map, false, false, true);
                if (changeDetected)
                {
                    op.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return map;
        }
        else if (mmd.hasArray())
        {
            // "a,b,c,d,..."
            AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
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
                // TODO handle interface[]
                String elementIdStr = (String)idIter.next();
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
                if (op != null)
                {
                    op.makeDirty(mmd.getAbsoluteFieldNumber());
                }
            }
            return array;
        }
        else
        {
            return value;
        }
    }

    protected Object getValueForContainerNonRelationField(AbstractMemberMetaData mmd, Object value, ColumnMetaData colmd)
    {
        if (mmd.getTypeConverterName() != null)
        {
            // User-defined type converter
            TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
            return conv.toMemberType(value);
        }

        Object fieldValue = MongoDBUtils.getFieldValueFromStored(ec, mmd, value, FieldRole.ROLE_FIELD);
        if (op != null)
        {
            // Wrap if SCO
            return op.wrapSCOField(mmd.getAbsoluteFieldNumber(), fieldValue, false, false, true);
        }
        return fieldValue;
    }

    private Object getMapKeyForReturnValue(AbstractMemberMetaData mmd, Object value)
    {
        String keyType = mmd.getMap().getKeyType();
        Class keyCls = ec.getClassLoaderResolver().classForName(keyType);
        if (keyCls == Long.class || keyCls == Double.class || keyCls == Float.class ||
            keyCls == Integer.class || keyCls == Short.class || keyCls == String.class)
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
        if (valueCls == Long.class || valueCls == Double.class || valueCls == Float.class ||
                valueCls == Integer.class || valueCls == Short.class || valueCls == String.class)
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
