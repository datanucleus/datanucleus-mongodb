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
package org.datanucleus.store.mongodb.query.expression;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.schema.table.MemberColumnMapping;

/**
 * Expression for a field in a MongoDB document.
 */
public class MongoFieldExpression extends MongoExpression
{
    MemberColumnMapping mapping;
    AbstractMemberMetaData mmd;
    String propertyName;

    public MongoFieldExpression(String propName, AbstractMemberMetaData mmd, MemberColumnMapping mapping)
    {
        this.propertyName = propName;
        this.mmd = mmd;
        this.mapping = mapping;
    }

    public MemberColumnMapping getMemberColumnMapping()
    {
        return mapping;
    }

    public AbstractMemberMetaData getMemberMetaData()
    {
        return mmd;
    }

    public String getPropertyName()
    {
        return propertyName;
    }

    public String toString()
    {
        return propertyName;
    }
}