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
   ...
**********************************************************************/
package org.datanucleus.store.mongodb.fieldmanager;

import com.mongodb.DBObject;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * FieldManager for the retrieval of a related embedded object.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents. */
    protected List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(ObjectProvider op, DBObject dbObject, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(op, dbObject, table);
        this.ownerMmd = (mmds != null ? mmds.get(mmds.size()-1) : null);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
            ObjectProvider[] ownerOps = ec.getOwnersForEmbeddedObjectProvider(op);
            return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
        }

        RelationType relationType = mmd.getRelationType(clr);
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        List<AbstractMemberMetaData> embMmds = new ArrayList<>(mmds);
        embMmds.add(mmd);

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // (Nested) Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                /*if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.MANY_TO_ONE_BI)
                {
                    if ((ownerMmd.getMappedBy() != null && embMmd.getName().equals(ownerMmd.getMappedBy())) ||
                        (embMmd.getMappedBy() != null && ownerMmd.getName().equals(embMmd.getMappedBy())))
                    {
                        // Other side of owner bidirectional, so return the owner
                        ObjectProvider[] ownerSms = op.getEmbeddedOwners();
                        if (ownerSms == null)
                        {
                            throw new NucleusException("Processing of " + embMmd.getFullFieldName() + " cannot set value to owner since owner ObjectProvider not set");
                        }
                        return ownerSms[0].getObject();
                    }
                }*/

                boolean nested = MongoDBUtils.isMemberNested(mmd);

                // Check for null value (currently need all columns to return null)
                DBObject subObject = dbObject;
                if (nested)
                {
                    subObject = (DBObject) dbObject.get(mapping.getColumn(0).getName());

                    if (subObject == null)
                    {
                        return null;
                    }
                }
                else
                {
                    // TODO Cater for null using embmd.getNullIndicatorColumn etc
                    if (embmd != null)
                    {
                        AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
                        boolean isNull = true;
                        for (int i=0;i<embmmds.length;i++)
                        {
                            String embFieldName = MongoDBUtils.getFieldName(ownerMmd, i);
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
                    }
                }

                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, subObject, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                ColumnMetaData[] columnMetaData = mapping.getMemberMetaData()
                        .getElementMetaData().getColumnMetaData();

                if (columnMetaData.length == 0) {
                    return null;
                }
                String embeddedCollectionColumn = columnMetaData[0].getName();
                DBObject subObject = (DBObject) dbObject.get(embeddedCollectionColumn);

                if (subObject == null)
                {
                    return null;
                }

                if (mmd.hasCollection())
                {
                    Collection<Object> coll;
                    AbstractClassMetaData elemCmd;

                    elemCmd = mmd.getCollection().getElementClassMetaData(clr);

                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    Collection collValue = (Collection) subObject;
                    for (Object aCollValue : collValue) {
                        DBObject elementObj = (DBObject) aCollValue;

                        ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, elemCmd, op, fieldNumber);
                        embOP.setPcObjectType(ObjectProvider.EMBEDDED_COLLECTION_ELEMENT_PC);

                        FetchFieldManager ffm = new FetchEmbeddedFieldManager(embOP, elementObj, embMmds, table);
                        ffm.ownerMmd = mmd;
                        embOP.replaceFields(elemCmd.getAllMemberPositions(), ffm);
                        coll.add(embOP.getObject());
                    }

                    return coll;
                }
                if (mmd.hasArray()) {
                    NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported as embedded array into the owning embedded object");
                }
                if (mmd.hasMap())
                {
                    NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported as embedded map into the owning embedded object");
                }
            }
            return null;
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }
}