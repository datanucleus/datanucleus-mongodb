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

/**
 * Operators for MongoDB queries.
 */
public enum MongoOperator 
{
    OP_EQ("$eq"),
    OP_NOTEQ("$ne"),
    OP_GT("$gt"),
    OP_GTEQ("$gte"),
    OP_LT("$lt"),
    OP_LTEQ("$lte"),
    OP_AND("$and"),
    OP_OR("$or"),
    REGEX("$regex"),
    IN("$in");

    String value;

    private MongoOperator(String op_str)
    {
        this.value = op_str;
    }

    public String getValue()
    {
        return value;
    }
}