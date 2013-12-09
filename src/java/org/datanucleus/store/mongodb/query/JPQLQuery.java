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
package org.datanucleus.store.mongodb.query;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.mongodb.query.expression.MongoBooleanExpression;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.QueryManager;
import org.datanucleus.store.query.QueryResult;
import org.datanucleus.util.NucleusLogger;

/**
 * Implementation of JPQL for MongoDB datastores.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    /** The compilation of the query for this datastore. Not applicable if totally in-memory. */
    protected transient MongoDBQueryCompilation datastoreCompilation = null;

    /**
     * Constructs a new query instance that uses the given execution context.
     * @param storeMgr StoreManager for this query
     * @param ec Execution Context
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec Execution Context
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec Execution Context
     * @param query The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    /**
     * Utility to remove any previous compilation of this Query.
     */
    protected void discardCompiled()
    {
        super.discardCompiled();

        datastoreCompilation = null;
    }

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    @Override
    protected boolean isCompiled()
    {
        if (evaluateInMemory())
        {
            // Don't need datastore compilation here since evaluating in-memory
            return compilation != null;
        }
        else
        {
            // Need both to be present to say "compiled"
            if (compilation == null || datastoreCompilation == null)
            {
                return false;
            }
            if (!datastoreCompilation.isPrecompilable())
            {
                NucleusLogger.GENERAL.info("Query compiled but not precompilable so ditching datastore compilation");
                datastoreCompilation = null;
                return false;
            }
            return true;
        }
    }

    /**
     * Convenience method to return whether the query should be evaluated in-memory.
     * @return Use in-memory evaluation?
     */
    protected boolean evaluateInMemory()
    {
        if (candidateCollection != null)
        {
            if (compilation != null && compilation.getSubqueryAliases() != null)
            {
                // TODO In-memory evaluation of subqueries isn't fully implemented yet, so remove this when it is
                NucleusLogger.QUERY.warn("In-memory evaluator doesn't currently handle subqueries completely so evaluating in datastore");
                return false;
            }

            Object val = getExtension(EXTENSION_EVALUATE_IN_MEMORY);
            if (val == null)
            {
                return true;
            }
            return Boolean.valueOf((String)val);
        }
        return super.evaluateInMemory();
    }

    /**
     * Method to compile the JDOQL query.
     * Uses the superclass to compile the generic query populating the "compilation", and then generates
     * the datastore-specific "datastoreCompilation".
     * @param parameterValues Map of param values keyed by param name (if available at compile time)
     */
    protected synchronized void compileInternal(Map parameterValues)
    {
        if (isCompiled())
        {
            return;
        }

        // Compile the generic query expressions
        super.compileInternal(parameterValues);

        boolean inMemory = evaluateInMemory();
        if (candidateCollection != null && inMemory)
        {
            // Querying a candidate collection in-memory, so just return now (don't need datastore compilation)
            // TODO Maybe apply the result class checks ?
            return;
        }

        if (candidateClass == null || candidateClassName == null)
        {
            candidateClass = compilation.getCandidateClass();
            candidateClassName = candidateClass.getName();
        }

        if (candidateClass == null)
        {
            throw new NucleusUserException(LOCALISER.msg("021009", candidateClassName));
        }

        // Make sure any persistence info is loaded
        ec.hasPersistenceInformationForClass(candidateClass);
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);

        QueryManager qm = getQueryManager();
        String datastoreKey = getStoreManager().getQueryCacheKey();
        String cacheKey = getQueryCacheKey();
        if (useCaching())
        {
            // Allowing caching so try to find compiled (datastore) query
            datastoreCompilation = (MongoDBQueryCompilation)qm.getDatastoreQueryCompilation(datastoreKey,
                getLanguage(), cacheKey);
            if (datastoreCompilation != null)
            {
                // Cached compilation exists for this datastore so reuse it
                setResultDistinct(compilation.getResultDistinct());
                return;
            }
        }

        datastoreCompilation = new MongoDBQueryCompilation();
        synchronized (datastoreCompilation)
        {
            if (inMemory)
            {
                // Generate statement to just retrieve all candidate objects for later processing
            }
            else
            {
                // Try to generate statement to perform the full query in the datastore
                compileQueryFull(parameterValues, cmd);
            }
        }

        if (cacheKey != null)
        {
            if (datastoreCompilation.isPrecompilable())
            {
                qm.addDatastoreQueryCompilation(datastoreKey, getLanguage(), cacheKey, datastoreCompilation);
            }
        }
    }
    
    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        try
        {
            DB db = (DB)mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JPQL", getSingleStringQuery(), null));
            }

            boolean filterInMemory = true;
            boolean rangeInMemory = (range != null);
            List candidates = null;
            if (candidateCollection != null)
            {
                candidates = new ArrayList(candidateCollection);
            }
            else if (evaluateInMemory())
            {
                candidates = MongoDBUtils.getObjectsOfCandidateType(this, db, null, null);
            }
            else
            {
                BasicDBObject filterObject = null;
                MongoBooleanExpression filterExpr = datastoreCompilation.getFilterExpression();
                if (filterExpr != null)
                {
                    filterObject = filterExpr.getDBObject();
                }
                
                MongoDBResult resultObject = datastoreCompilation.getResult();
                // TODO properly support GROUP BY
                if (resultObject != null && resultObject.isCountOnly() && datastoreCompilation.isFilterComplete() && 
                    grouping == null) 
                {
                    return MongoDBUtils.performMongoCount(db, filterObject, candidateClass, subclasses, ec);
                } 
                else 
                {
                    Map<String, Object> options = new HashMap();
                    if (getBooleanExtensionProperty("slave-ok", false))
                    {
                        options.put("slave-ok", true);
                    }

                    if (filter == null || datastoreCompilation.isFilterComplete())
                    {
                        filterInMemory = false;
                    }

                    if (filterInMemory || result != null || resultClass != null)
                    {
                        candidates = MongoDBUtils.getObjectsOfCandidateType(this, db, filterObject, options);
                    }
                    else
                    {
                        // Execute as much as possible in the datastore
                        BasicDBObject orderingObject = datastoreCompilation.getOrdering();
                        candidates = MongoDBUtils.getObjectsOfCandidateType(this, db, filterObject, orderingObject, options,
                            (int) this.fromInclNo - 1, (int) (this.toExclNo - this.fromInclNo));
                    }
                }
            }

            Collection results = candidates;
            if (filterInMemory || result != null || resultClass != null || rangeInMemory)
            {
                if (results instanceof QueryResult)
                {
                    // Make sure the cursor(s) are all loaded
                    ((QueryResult)results).disconnect();
                }

                // Perform any additional evaluation in-memory
                JavaQueryEvaluator resultMapper = new JPQLEvaluator(this, candidates, compilation, 
                    parameters, ec.getClassLoaderResolver());
                results = resultMapper.execute(filterInMemory, result != null, resultClass != null, true, rangeInMemory);
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JPQL", 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == BULK_DELETE)
            {
                if (results instanceof QueryResult)
                {
                    // Make sure the cursor(s) are all loaded
                    ((QueryResult)results).disconnect();
                }

                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == BULK_UPDATE)
            {
                throw new NucleusException("Bulk Update is not yet supported");
            }

            if (results instanceof QueryResult)
            {
                final QueryResult qr1 = (QueryResult)results;
                final ManagedConnection mconn1 = mconn;
                ManagedConnectionResourceListener listener =
                    new ManagedConnectionResourceListener()
                {
                    public void transactionFlushed(){}
                    public void transactionPreClose()
                    {
                        // Tx : disconnect query from ManagedConnection (read in unread rows etc)
                        qr1.disconnect();
                    }
                    public void managedConnectionPreClose()
                    {
                        if (!ec.getTransaction().isActive())
                        {
                            // Non-Tx : disconnect query from ManagedConnection (read in unread rows etc)
                            qr1.disconnect();
                        }
                    }
                    public void managedConnectionPostClose(){}
                    public void resourcePostClose()
                    {
                        mconn1.removeListener(this);
                    }
                };
                mconn.addListener(listener);
                if (qr1 instanceof AbstractQueryResult)
                {
                    ((AbstractQueryResult)qr1).addConnectionListener(listener);
                }
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to compile the query for the datastore attempting to evaluate the whole query in the datastore
     * if possible. Sets the components of the "datastoreCompilation".
     * @param parameters Input parameters (if known)
     * @param candidateCmd Metadata for the candidate class
     */
    private void compileQueryFull(Map parameters, AbstractClassMetaData candidateCmd)
    {
        long startTime = 0;
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
            NucleusLogger.QUERY.debug(LOCALISER.msg("021083", getLanguage(), toString()));
        }

        // Generate statement for candidate(s)

        // Update the SQLStatement with filter, ordering, result etc
        QueryToMongoDBMapper mapper = new QueryToMongoDBMapper(compilation, parameters, candidateCmd, ec, this);
        mapper.compile();
        datastoreCompilation.setFilterComplete(mapper.isFilterComplete());
        datastoreCompilation.setFilterExpression(mapper.getFilterExpression());
        datastoreCompilation.setResultComplete(mapper.isResultComplete());
        datastoreCompilation.setResult(mapper.getResultObject());
        datastoreCompilation.setPrecompilable(mapper.isPrecompilable());

        if (candidateCollection != null)
        {
            // Restrict to the supplied candidate ids
        }

        // Apply any range
        if (range != null)
        {
        }

        // Set any extensions (TODO Support locking if possible with MongoDB)


        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(LOCALISER.msg("021084", getLanguage(), System.currentTimeMillis()-startTime));
        }
    }
}