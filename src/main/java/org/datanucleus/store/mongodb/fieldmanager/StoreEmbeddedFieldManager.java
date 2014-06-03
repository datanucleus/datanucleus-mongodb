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

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;

import com.mongodb.DBObject;

/**
 * FieldManager for the persistence of a related embedded object (1-1/N-1 relation).
 * This handles flat embedding of related embedded objects, where the field of the embedded object become
 * a field in the owner document.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    // TODO Pass in mmds rather than ownerMmd (see Cassandra plugin for an example)
    public StoreEmbeddedFieldManager(ObjectProvider op, DBObject dbObject, AbstractMemberMetaData ownerMmd, boolean insert, Table table)
    {
        super(op, dbObject, insert, table);
        this.ownerMmd = ownerMmd;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    protected String getFieldName(int fieldNumber)
    {
        return MongoDBUtils.getFieldName(ownerMmd, fieldNumber);
    }

    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData embMmd = ownerMmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
        if (!isStorable(embMmd))
        {
            return;
        }

        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = embMmd.getRelationType(clr);
        if (embMmd.isEmbedded() && RelationType.isRelationSingleValued(relationType))
        {
            // Embedded PC object - This performs "flat embedding" as fields in the same document
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embMmd.getType(), clr);
            if (embcmd == null)
            {
                throw new NucleusUserException("Field " + embMmd.getFullFieldName() +
                    " specified as embedded but metadata not found for the class of type " + embMmd.getTypeName());
            }

            if (value == null)
            {
                // TODO Delete any fields for the embedded object
                return;
            }

            if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.MANY_TO_ONE_BI)
            {
                if ((ownerMmd.getMappedBy() != null && embMmd.getName().equals(ownerMmd.getMappedBy())) ||
                    (embMmd.getMappedBy() != null && ownerMmd.getName().equals(embMmd.getMappedBy())))
                {
                    // Other side of owner bidirectional, so omit
                    return;
                }
            }

            // Process all fields of the embedded object
            ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, embMmd);
            FieldManager ffm = new StoreEmbeddedFieldManager(embOP, dbObject, embMmd, insert, table);
            embOP.provideFields(embcmd.getAllMemberPositions(), ffm);
            return;
        }

        String fieldName = MongoDBUtils.getFieldName(ownerMmd, fieldNumber);
        if (value == null)
        {
            if (dbObject.containsField(fieldName))
            {
                dbObject.removeField(fieldName);
            }
            return;
        }

        if (embMmd.isSerialized())
        {
            // TODO Allow other types of serialisation
            byte[] bytes = MongoDBUtils.getStoredValueForJavaSerialisedField(embMmd, value);
            dbObject.put(fieldName, bytes);
            op.wrapSCOField(fieldNumber, value, false, false, true);
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // PC object
            processSingleRelationField(value, ec, fieldName);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            processContainerRelationField(embMmd, value, ec, fieldName);
        }
        else
        {
            processContainerNonRelationField(fieldName, ec, value, dbObject, embMmd, FieldRole.ROLE_FIELD);
        }
    }
}