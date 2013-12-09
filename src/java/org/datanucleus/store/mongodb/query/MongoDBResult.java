/**********************************************************************
Copyright (c) 2012 Chris Rued and others. All rights reserved.
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

import com.mongodb.BasicDBObject;

public class MongoDBResult
{
    private boolean countOnly;

    private BasicDBObject mongoDbObject;

    public MongoDBResult()
    {
        this(null, false);
    }

    public MongoDBResult(BasicDBObject mongoDbObject)
    {
        this(mongoDbObject, false);
    }

    public MongoDBResult(BasicDBObject mongoDbObject, boolean countOnly)
    {
        this.countOnly = countOnly;
        this.mongoDbObject = mongoDbObject;
    }

    public boolean isCountOnly()
    {
        return countOnly;
    }

    public void setCountOnly(boolean countOnly)
    {
        this.countOnly = countOnly;
    }

    public BasicDBObject getMongoDbObject()
    {
        return mongoDbObject;
    }

    public void setMongoDbObject(BasicDBObject mongoDbObject)
    {
        this.mongoDbObject = mongoDbObject;
    }
}
