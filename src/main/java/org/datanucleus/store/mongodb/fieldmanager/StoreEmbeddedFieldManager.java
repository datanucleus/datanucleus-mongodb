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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * FieldManager for the persistence of a related embedded object (1-1/N-1 relation).
 * This handles flat embedding of related embedded objects, where the field of the embedded object become a field in the owner document.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents. */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(DNStateManager sm, DBObject dbObject, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, dbObject, insert, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMemberMetaData lastMmd = mmds.get(mmds.size()-1);

        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this member being a link back to the owner. TODO Repeat this for nested and their owners
            DNStateManager[] ownerSMs = ec.getOwnersForEmbeddedStateManager(sm);
            if (ownerSMs != null && ownerSMs.length == 1 && value != ownerSMs[0].getObject())
            {
                // Make sure the owner field is set
                sm.replaceField(fieldNumber, ownerSMs[0].getObject());
            }
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        List<AbstractMemberMetaData> embMmds = new ArrayList<>(mmds);
        embMmds.add(mmd);

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, lastMmd))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object - This performs "flat embedding" as fields in the same document
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (embCmd == null)
                {
                    throw new NucleusUserException("Member " + mmd.getFullFieldName() +
                        " specified as embedded but metadata not found for the class of type " + mmd.getTypeName());
                }

                // Embedded PC object - can be stored nested in the BSON doc (default), or flat

                if (RelationType.isBidirectional(relationType))
                {
                    // TODO Add logic for bidirectional relations so we know when to stop embedding
                }

                if (value == null)
                {
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        dbObject.removeField(mapping.getColumn(i).getName());
                    }
                    return;
                }

                boolean nested = MongoDBUtils.isMemberNested(mmd);

                DBObject obj;
                if (nested)
                {
                    obj = new BasicDBObject();
                }
                else
                {
                    obj = dbObject;
                }

                DNStateManager embSM = ec.findStateManagerForEmbedded(value, sm, mmd, PersistableObjectType.EMBEDDED_PC);
                FieldManager ffm = new StoreEmbeddedFieldManager(embSM, obj, insert, embMmds, table);
                embSM.provideFields(embCmd.getAllMemberPositions(), ffm);

                if (nested) {
                    dbObject.put(mapping.getColumn(0).getName(), obj);
                }
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                ColumnMetaData[] embeddedColumns = mapping.getMemberMetaData().getElementMetaData().getColumnMetaData();

                if (embeddedColumns.length == 0)
                {
                    return;
                }

                String fieldName = embeddedColumns[0].getName();

                if (value == null)
                {
                    dbObject.removeField(fieldName);
                    return;
                }

                if (mmd.hasCollection())
                {
                    AbstractClassMetaData embcmd = mmd.getCollection().getElementClassMetaData(clr);
                    Collection coll = new ArrayList();
                    Collection valueColl = (Collection) value;
                    Iterator collIter = valueColl.iterator();
                    while (collIter.hasNext())
                    {
                        Object element = collIter.next();
                        if (!element.getClass().getName().equals(embcmd.getFullClassName()))
                        {
                            // Inherited object
                            embcmd = ec.getMetaDataManager().getMetaDataForClass(element.getClass(), clr);
                        }

                        BasicDBObject embeddedObject = new BasicDBObject();

                        DNStateManager embSM = ec.findStateManagerForEmbedded(element, sm, mmd, PersistableObjectType.EMBEDDED_COLLECTION_ELEMENT_PC);
                        StoreFieldManager sfm = new StoreEmbeddedFieldManager(embSM, embeddedObject, insert, embMmds, table);
                        sfm.ownerMmd = mmd;
                        embSM.provideFields(embcmd.getAllMemberPositions(), sfm);
                        coll.add(embeddedObject);
                    }
                    dbObject.put(fieldName, coll);
                    return;
                }

                if (mmd.hasArray())
                {
                    throw new NucleusException("Member " + mmd.getFullFieldName() + " is embedded but we do not support embedded array fields in this location (owner=" + sm + ")");
                }

                if (mmd.hasMap())
                {
                    throw new NucleusException("Member " + mmd.getFullFieldName() + " is embedded but we do not support embedded map fields in this location (owner=" + sm + ")");
                }
            }
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }
}