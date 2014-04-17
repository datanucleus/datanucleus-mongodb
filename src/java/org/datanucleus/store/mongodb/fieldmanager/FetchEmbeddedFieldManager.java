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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mongodb.MongoDBUtils;

import com.mongodb.DBObject;

/**
 * FieldManager for the retrieval of a related embedded object (1-1 relation).
 * This handles flat embedding of related embedded objects, where the field of the embedded object become
 * a field in the owner document.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    private final AbstractMemberMetaData ownerMmd;

    public FetchEmbeddedFieldManager(ObjectProvider op, DBObject dbObject, AbstractMemberMetaData ownerMmd)
    {
        super(op, dbObject);
        this.ownerMmd = ownerMmd;
    }

    protected String getFieldName(int fieldNumber)
    {
        return MongoDBUtils.getFieldName(ownerMmd, fieldNumber);
    }

    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData embMmd = ownerMmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
        RelationType relationType = embMmd.getRelationType(clr);
        if (RelationType.isRelationSingleValued(relationType) && embMmd.isEmbedded())
        {
            // Persistable object embedded into table of this object
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embMmd.getType(), clr);
            if (embcmd == null)
            {
                throw new NucleusUserException("Field " + ownerMmd.getFullFieldName() +
                    " marked as embedded but no such metadata");
            }

            if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.MANY_TO_ONE_BI)
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
            }

            // Check for null value (currently need all columns to return null)
            // TODO Cater for null using embmd.getNullIndicatorColumn etc
            EmbeddedMetaData embmd = ownerMmd.getEmbeddedMetaData();
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

            ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embcmd, op, fieldNumber);
            FieldManager ffm = new FetchEmbeddedFieldManager(embOP, dbObject, embMmd);
            embOP.replaceFields(embcmd.getAllMemberPositions(), ffm);
            return embOP.getObject();
        }

        String fieldName = MongoDBUtils.getFieldName(ownerMmd, fieldNumber);
        if (!dbObject.containsField(fieldName))
        {
            return null;
        }

        Object value = dbObject.get(fieldName);
        if (embMmd.isSerialized())
        {
            // TODO Allow other types of serialisation
            Object returnValue = MongoDBUtils.getFieldValueForJavaSerialisedField(embMmd, value);
            if (op != null)
            {
                // Wrap if SCO
                returnValue = op.wrapSCOField(embMmd.getAbsoluteFieldNumber(), returnValue, false, false, true);
            }
            return returnValue;
        }
        if (RelationType.isRelationSingleValued(relationType))
        {
            return getValueForSingleRelationField(embMmd, value, clr);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            return getValueForContainerRelationField(embMmd, value, clr);
        }
        else
        {
            ColumnMetaData colmd = null;
            if (embMmd.getColumnMetaData() != null)
            {
                colmd = embMmd.getColumnMetaData()[0];
            }
            return getValueForContainerNonRelationField(embMmd, value, colmd);
        }
    }
}