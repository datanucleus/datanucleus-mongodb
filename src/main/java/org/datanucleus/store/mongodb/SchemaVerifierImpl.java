/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.mongodb;

import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SchemaVerifier;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Implementation of a schema verifier for Excel.
 * This class provides a way for the Excel plugin to override any "default" handling that core provides to better fit in with the types
 * that are persistable in Excel.
 */
public class SchemaVerifierImpl implements SchemaVerifier
{
    StoreManager storeMgr;
    AbstractClassMetaData cmd;
    ClassLoaderResolver clr;

    public SchemaVerifierImpl(StoreManager storeMgr, AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        this.storeMgr = storeMgr;
        this.cmd = cmd;
        this.clr = clr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.SchemaVerifier#verifyTypeConverterForMember(org.datanucleus.metadata.AbstractMemberMetaData, org.datanucleus.store.types.converters.TypeConverter)
     */
    @Override
    public TypeConverter verifyTypeConverterForMember(AbstractMemberMetaData mmd, TypeConverter conv)
    {
        // TODO Override any type handling that Excel would do differently
        return conv;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.SchemaVerifier#attributeMember(org.datanucleus.store.schema.table.MemberColumnMapping)
     */
    @Override
    public void attributeMember(MemberColumnMapping mapping)
    {
        // TODO Add any information to the Column that may be useful later
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.SchemaVerifier#attributeColumn(org.datanucleus.store.schema.table.MemberColumnMapping, org.datanucleus.metadata.AbstractMemberMetaData)
     */
    @Override
    public void attributeMember(MemberColumnMapping mapping, AbstractMemberMetaData mmd)
    {
        // TODO Add any information to the Column that may be useful later
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.SchemaVerifier#attributeEmbeddedColumn(org.datanucleus.store.schema.table.MemberColumnMapping, java.util.List)
     */
    @Override
    public void attributeEmbeddedMember(MemberColumnMapping mapping, List<AbstractMemberMetaData> mmds)
    {
        // TODO Add any information to the Column that may be useful later
    }
}