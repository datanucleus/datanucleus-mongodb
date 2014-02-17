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

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.mongodb.valuegenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Generator that uses a collection in MongoDB to store and allocate identity values.
 */
public class IncrementGenerator extends AbstractDatastoreGenerator implements ValueGenerator
{
    static final String INCREMENT_COL_NAME = "increment";

    /** Key used in the Table to access the increment count */
    private String key;

    private String collectionName = null;

    /**
     * Constructor. Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * @param name Symbolic name for this generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(String name, Properties props)
    {
        super(name, props);
        this.key = properties.getProperty("field-name", name);
        this.collectionName = properties.getProperty("sequence-table-name");
        if (this.collectionName == null)
        {
            this.collectionName = "IncrementTable";
        }
        if (properties.containsKey("key-cache-size"))
        {
            allocationSize = Integer.valueOf(properties.getProperty("key-cache-size"));
        }
        else
        {
            allocationSize = 1;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    protected ValueGenerationBlock reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        List oids = new ArrayList();
        ManagedConnection mconn = connectionProvider.retrieveConnection();
        try
        {
            DB db = (DB)mconn.getConnection();
            if (!storeMgr.getSchemaHandler().isAutoCreateTables() && !db.collectionExists(collectionName))
            {
                throw new NucleusUserException(LOCALISER.msg("040011", collectionName));
            }

            // Create the collection if not existing
            DBCollection dbCollection = db.getCollection(collectionName);
            BasicDBObject query = new BasicDBObject();
            query.put("field-name", key);
            DBCursor curs = dbCollection.find(query);
            if (curs == null || !curs.hasNext())
            {
                // No current entry for this key, so add initial entry
                long initialValue = 0;
                if (properties.containsKey("key-initial-value"))
                {
                    initialValue = Long.valueOf(properties.getProperty("key-initial-value"))-1;
                }
                BasicDBObject dbObject = new BasicDBObject();
                dbObject.put("field-name", key);
                dbObject.put(INCREMENT_COL_NAME, new Long(initialValue));
                dbCollection.insert(dbObject);
            }

            // Create the entry for this field if not existing
            query = new BasicDBObject();
            query.put("field-name", key);
            curs = dbCollection.find(query);
            DBObject dbObject = curs.next();

            Long currentValue = (Long)dbObject.get(INCREMENT_COL_NAME);
            long number = currentValue.longValue();
            for (int i=0;i<size;i++)
            {
                oids.add(number + i + 1);
            }
            dbObject.put(INCREMENT_COL_NAME, new Long(number+size));
            dbCollection.save(dbObject);
        }
        finally
        {
            connectionProvider.releaseConnection();
        }

        return new ValueGenerationBlock(oids);
    }
}