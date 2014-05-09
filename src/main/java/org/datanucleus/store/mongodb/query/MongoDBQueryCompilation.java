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
package org.datanucleus.store.mongodb.query;

import org.datanucleus.store.mongodb.query.expression.MongoBooleanExpression;

import com.mongodb.BasicDBObject;

/**
 * Datastore-specific (MongoDB) compilation information for a java query.
 */
public class MongoDBQueryCompilation
{
    boolean filterComplete = true;

    /** Expression defining the filter (if any). */
    MongoBooleanExpression filterExpr;

    BasicDBObject orderingObject;

    boolean resultComplete = true;

    boolean precompilable = true;

    /** Object defining document fields to be returned (if any). */
    MongoDBResult resultObject;

    public MongoDBQueryCompilation()
    {
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public void setFilterComplete(boolean complete)
    {
        this.filterComplete = complete;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void setPrecompilable(boolean flag)
    {
        this.precompilable = flag;
    }

    public void setFilterExpression(MongoBooleanExpression filterExpr)
    {
        this.filterExpr = filterExpr;
    }

    public MongoBooleanExpression getFilterExpression()
    {
        return filterExpr;
    }

    public void setOrdering(BasicDBObject ordering)
    {
        this.orderingObject = ordering;
    }

    public BasicDBObject getOrdering()
    {
        return orderingObject;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public void setResultComplete(boolean complete)
    {
        this.resultComplete = complete;
    }

    public void setResult(MongoDBResult result)
    {
        this.resultObject = result;
    }

    public MongoDBResult getResult()
    {
        return resultObject;
    }
}