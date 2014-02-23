/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.SoftValueMap;
import org.datanucleus.util.StringUtils;
import org.datanucleus.util.WeakValueMap;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * QueryResult for MongoDB queries that tries to lazy load results from the provided DBCursor(s)
 * so to avoid problems with memory.
 */
public class LazyLoadQueryResult extends AbstractQueryResult implements Serializable
{
    protected ExecutionContext ec;

    /** Candidate results for this query that have not been (completely) processed yet. */
    protected List<CandidateClassResult> candidateResults = new ArrayList<CandidateClassResult>();

    /** Iterator for the DBCursor that is being processed (if any). */
    protected Iterator<DBObject> currentCursorIterator = null;

    /** Map of object, keyed by the index (0, 1, etc). */
    protected Map<Integer, Object> itemsByIndex = null;

    boolean rangeProcessed = false;

    boolean orderProcessed = false;

    public LazyLoadQueryResult(Query q)
    {
        super(q);
        this.ec = q.getExecutionContext();

        // Process any supported extensions
        String cacheType = query.getStringExtensionProperty("cacheType", "strong");
        if (cacheType != null)
        {
            if (cacheType.equalsIgnoreCase("soft"))
            {
                itemsByIndex = new SoftValueMap();
            }
            else if (cacheType.equalsIgnoreCase("weak"))
            {
                itemsByIndex = new WeakValueMap();
            }
            else if (cacheType.equalsIgnoreCase("strong"))
            {
                itemsByIndex = new HashMap();
            }
            else if (cacheType.equalsIgnoreCase("none"))
            {
                itemsByIndex = null;
            }
            else
            {
                itemsByIndex = new WeakValueMap();
            }
        }
        else
        {
            itemsByIndex = new WeakValueMap();
        }
    }

    private class CandidateClassResult
    {
        AbstractClassMetaData cmd;
        DBCursor cursor;
        int[] fpMembers;
        public CandidateClassResult(AbstractClassMetaData cmd, DBCursor curs, int[] fpMemberPositions)
        {
            this.cmd = cmd;
            this.cursor = curs;
            this.fpMembers = fpMemberPositions;
        }
    }

    public void addCandidateResult(AbstractClassMetaData cmd, DBCursor cursor, int[] fpMembers)
    {
        candidateResults.add(new CandidateClassResult(cmd, cursor, fpMembers));
    }

    /**
     * Allow the creator to signify that the query range was processed when generating this QueryResult
     * @param processed Whether the range was processed when the query was executed
     */
    public void setRangeProcessed(boolean processed)
    {
        this.rangeProcessed = processed;
    }

    public boolean getRangeProcessed()
    {
        return rangeProcessed;
    }

    /**
     * Allow the creator to signify that the query order was processed when generating this QueryResult
     * @param processed Whether the order was processed when the query was executed
     */
    public void setOrderProcessed(boolean processed)
    {
        this.orderProcessed = processed;
    }

    public boolean getOrderProcessed()
    {
        return orderProcessed;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#closingConnection()
     */
    @Override
    protected void closingConnection()
    {
        if (loadResultsAtCommit && isOpen() && !candidateResults.isEmpty())
        {
            // Query connection closing message
            NucleusLogger.QUERY.info(LOCALISER.msg("052606", query.toString()));

            synchronized (this)
            {
                if (currentCursorIterator != null)
                {
                    // Finish off the current cursor
                    CandidateClassResult result = candidateResults.get(0);
                    while (currentCursorIterator.hasNext())
                    {
                        DBObject dbObject = currentCursorIterator.next();
                        Object pojo = MongoDBUtils.getPojoForDBObjectForCandidate(dbObject, ec, result.cmd,
                            result.fpMembers, query.getIgnoreCache());
                        itemsByIndex.put(itemsByIndex.size(), pojo);
                    }
                    result.cursor.close();
                    candidateResults.remove(result);
                    currentCursorIterator = null;
                }

                // Process remaining cursors
                Iterator<CandidateClassResult> candidateResultsIter = candidateResults.iterator();
                while (candidateResultsIter.hasNext())
                {
                    CandidateClassResult result = candidateResultsIter.next();
                    currentCursorIterator = result.cursor.iterator();
                    while (currentCursorIterator.hasNext())
                    {
                        DBObject dbObject = currentCursorIterator.next();
                        Object pojo = MongoDBUtils.getPojoForDBObjectForCandidate(dbObject, ec, result.cmd,
                            result.fpMembers, query.getIgnoreCache());
                        itemsByIndex.put(itemsByIndex.size(), pojo);
                    }
                    result.cursor.close();
                    candidateResultsIter.remove();
                    currentCursorIterator = null;
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#close()
     */
    @Override
    public synchronized void close()
    {
        itemsByIndex.clear();
        itemsByIndex = null;

        candidateResults = null;

        super.close();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#closeResults()
     */
    @Override
    protected void closeResults()
    {
        // TODO Cache any query results if required
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#getSizeUsingMethod()
     */
    @Override
    protected int getSizeUsingMethod()
    {
        if (resultSizeMethod.equalsIgnoreCase("LAST"))
        {
            // Just load all results and the size is the number we have
            while (true)
            {
                getNextObject();
                if (candidateResults.isEmpty())
                {
                    size = itemsByIndex.size();
                    return size;
                }
            }
        }

        return super.getSizeUsingMethod();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#get(int)
     */
    @Override
    public Object get(int index)
    {
        if (index < 0)
        {
            throw new IndexOutOfBoundsException("Index must be 0 or higher");
        }

        if (itemsByIndex != null && itemsByIndex.containsKey(index))
        {
            return itemsByIndex.get(index);
        }
        else
        {
            // Load next object continually until we find it
            while (true)
            {
                Object nextPojo = getNextObject();
                if (itemsByIndex.size() == (index+1))
                {
                    return nextPojo;
                }
                if (candidateResults.isEmpty())
                {
                    throw new IndexOutOfBoundsException("Beyond size of the results (" + itemsByIndex.size() + ")");
                }
            }
        }
    }

    /**
     * Method to extract the next object from the candidateResults (if there is one).
     * If a result is present, this puts it into "itemsByIndex".
     * Returns null if no more results.
     * @return The next result (or null if no more).
     */
    protected Object getNextObject()
    {
        if (candidateResults.isEmpty())
        {
            // Already exhausted
            return null;
        }

        Object pojo = null;
        CandidateClassResult result = candidateResults.get(0);

        if (currentCursorIterator != null)
        {
            // Continue the current cursor iterator
            DBObject dbObject = currentCursorIterator.next();
            pojo = MongoDBUtils.getPojoForDBObjectForCandidate(dbObject, ec, result.cmd,
                result.fpMembers, query.getIgnoreCache());
            itemsByIndex.put(itemsByIndex.size(), pojo);

            if (!currentCursorIterator.hasNext())
            {
                // Reached end of Cursor, so close the current iterator/cursor and remove the result
                result.cursor.close();
                currentCursorIterator = null;
                candidateResults.remove(result);
            }
        }
        else
        {
            // Start next candidate result
            boolean noNextResult = true;
            while (noNextResult)
            {
                currentCursorIterator = result.cursor.iterator();
                if (currentCursorIterator.hasNext())
                {
                    DBObject dbObject = currentCursorIterator.next();
                    pojo = MongoDBUtils.getPojoForDBObjectForCandidate(dbObject, ec, result.cmd,
                        result.fpMembers, query.getIgnoreCache());
                    itemsByIndex.put(itemsByIndex.size(), pojo);
                    noNextResult = false;

                    if (!currentCursorIterator.hasNext())
                    {
                        // Reached end of Cursor, so close the current iterator/cursor and remove the result
                        result.cursor.close();
                        currentCursorIterator = null;
                        candidateResults.remove(result);
                    }
                }
                else
                {
                    // Cursor had no results so close and move on
                    result.cursor.close();
                    currentCursorIterator = null;
                    candidateResults.remove(result);
                    if (candidateResults.isEmpty())
                    {
                        // Break out since no more results
                        noNextResult = false;
                        pojo = null;
                    }
                    else
                    {
                        // Move on to next result
                        result = candidateResults.get(0);
                    }
                }
            }
        }

        return pojo;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#iterator()
     */
    @Override
    public Iterator iterator()
    {
        return new QueryResultIterator();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#listIterator()
     */
    @Override
    public ListIterator listIterator()
    {
        return new QueryResultIterator();
    }

    private class QueryResultIterator extends AbstractQueryResultIterator
    {
        private int nextRowNum = 0;

        @Override
        public boolean hasNext()
        {
            synchronized (LazyLoadQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                if (nextRowNum < itemsByIndex.size())
                {
                    return true;
                }

                return !candidateResults.isEmpty();
            }
        }

        @Override
        public Object next()
        {
            synchronized (LazyLoadQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(LOCALISER.msg("052600"));
                }

                if (nextRowNum < itemsByIndex.size())
                {
                    Object pojo = itemsByIndex.get(nextRowNum);
                    ++nextRowNum;
                    return pojo;
                }
                else if (!candidateResults.isEmpty())
                {
                    Object pojo = getNextObject();
                    ++nextRowNum;
                    return pojo;
                }
                throw new NoSuchElementException(LOCALISER.msg("052602"));
            }
        }

        @Override
        public boolean hasPrevious()
        {
            // We only navigate in forward direction, but maybe could provide this method
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public int nextIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public Object previous()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public int previousIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof LazyLoadQueryResult))
        {
            return false;
        }

        LazyLoadQueryResult other = (LazyLoadQueryResult)o;
        if (candidateResults != null)
        {
            return other.candidateResults.equals(candidateResults);
        }
        else if (query != null)
        {
            return other.query == query;
        }
        return StringUtils.toJVMIDString(other).equals(StringUtils.toJVMIDString(this));
    }

    public int hashCode()
    {
        return super.hashCode();
    }

    /**
     * Handle serialisation by returning a java.util.ArrayList of all of the results for this query
     * after disconnecting the query which has the consequence of enforcing the load of all objects.
     * @return The object to serialise
     * @throws ObjectStreamException
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        disconnect();
        List list = new ArrayList();
        for (int i=0;i<itemsByIndex.size();i++)
        {
            list.add(itemsByIndex.get(i));
        }
        return list;
    }
}