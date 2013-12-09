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
package org.datanucleus.store.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

/**
 * Implementation of a ConnectionFactory for MongoDB.
 * Accepts a URL of the form 
 * <pre>mongodb:[{server1}][/{dbName}][,{server2}[,{server3}]]</pre>
 * Defaults to a server of "localhost" if nothing specified
 * Defaults to a DB name of "DataNucleus" if nothing specified
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.mongodb.Localisation", MongoDBStoreManager.class.getClassLoader());

    String dbName = "DataNucleus";

    Mongo mongo;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // "mongodb:[server]"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property 'datanucleus.ConnectionURL' (or alias)");
        }

        String remains = url.substring(7).trim();
        if (remains.indexOf(':') == 0)
        {
            remains = remains.substring(1);
        }

        // Split into any replica sets
        try
        {
            List<ServerAddress> serverAddrs = new ArrayList<ServerAddress>();
            if (remains.length() == 0)
            {
                // "mongodb:"
                serverAddrs.add(new ServerAddress());
            }
            else
            {
                StringTokenizer tokeniser = new StringTokenizer(remains, ",");
                boolean firstServer = true;
                while (tokeniser.hasMoreTokens())
                {
                    String token = tokeniser.nextToken();

                    String serverName = "localhost";
                    if (firstServer)
                    {
                    	// Set dbName
                        int dbNameSepPos = token.indexOf("/");
                    	if (dbNameSepPos >= 0)
                    	{
                    		if (dbNameSepPos < token.length())
                    		{
                    			String dbNameStr = token.substring(dbNameSepPos+1);
                    			if (dbNameStr.length() > 0)
                    			{
                    				dbName = dbNameStr;
                    			}
                    			else
                    			{
                    				// Use default ("DataNucleus")
                    			}
                    		}
                    		if (dbNameSepPos > 0)
                    		{
                    			// Server name is not empty so use it
                    			serverName = token.substring(0, dbNameSepPos);
                    		}
                    	}
                    	else
                    	{
                    		if (token.length() > 0)
                    		{
                        		// No "/" specified so just take all of token
                    		    serverName = token;
                    		}
                    	}
                    }
                    else
                    {
                    	// Subsequent replica-set so use full token as server name
                    	serverName = token;
                    }

                    // Create a ServerAddress for this specification
                    ServerAddress addr = null;
                    int portSeparatorPos = serverName.indexOf(':');
                    if (portSeparatorPos > 0)
                    {
                        addr = new ServerAddress(serverName.substring(0, portSeparatorPos), 
                            Integer.valueOf(serverName.substring(portSeparatorPos+1)).intValue());
                    }
                    else
                    {
                        addr = new ServerAddress(serverName);
                    }
                    serverAddrs.add(addr);

                    firstServer = false;
                }
            }

            // Create the Mongo connection pool
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug(LOCALISER.msg("MongoDB.ServerConnect", dbName, serverAddrs.size(),
                    StringUtils.collectionToString(serverAddrs)));
            }
            // TODO Update this to MongoDB Java driver 2.10 and above
            if (serverAddrs.size() == 1)
            {
                mongo = new Mongo(serverAddrs.get(0), getMongodbOptions(storeMgr));
            }
            else
            {
                mongo = new Mongo(serverAddrs, getMongodbOptions(storeMgr));
            }
        }
        catch (UnknownHostException e)
        {
            throw new NucleusDataStoreException("Unable to connect to mongodb", e);
        }
        catch (MongoException me)
        {
            throw new NucleusDataStoreException("Unable to connect to mongodb", me);
        }
    }

    private MongoOptions getMongodbOptions(StoreManager storeManager)
    {
        Object connectionsPerHost = storeManager.getProperty("datanucleus.mongodb.connectionsPerHost");
        Object threadsAllowedToBlockForConnectionMultiplier = storeManager.getProperty("datanucleus.mongodb.threadsAllowedToBlockForConnectionMultiplier");
        MongoOptions mongoOptions = new MongoOptions();
        if (connectionsPerHost != null)
        {
            mongoOptions.connectionsPerHost = Integer.parseInt((String) connectionsPerHost);
        }
        if (threadsAllowedToBlockForConnectionMultiplier != null)
        {
            mongoOptions.threadsAllowedToBlockForConnectionMultiplier = Integer.parseInt((String) threadsAllowedToBlockForConnectionMultiplier);
        }
        return mongoOptions;
    }

    public void close()
    {
        mongo.close();
        super.close();
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction
     * associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param txnOptionsIgnored Any options for then creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map txnOptionsIgnored)
    {
        return new ManagedConnectionImpl();
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        boolean startRequested = false;
        XAResource xaRes = null;

        public ManagedConnectionImpl()
        {
        }

        /* (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#closeAfterTransactionEnd()
         */
        @Override
        public boolean closeAfterTransactionEnd()
        {
            // Don't call close() immediately after transaction commit/rollback/end since we want to
            // hang on to the connection until the ExecutionContext ends
            return false;
        }

        public Object getConnection()
        {
            if (conn == null || !startRequested)
            {
                obtainNewConnection();
            }
            return conn;
        }

        protected void obtainNewConnection()
        {
            if (conn == null)
            {
                // Create new connection
                conn = mongo.getDB(dbName);
                String userName = storeMgr.getConnectionUserName();
                String password = storeMgr.getConnectionPassword();
                if (!StringUtils.isWhitespace(userName))
                {
                    boolean authenticated = false;
                    if (!((DB)conn).isAuthenticated())
                    {
                        authenticated = ((DB)conn).authenticate(userName, password.toCharArray());
                        if (!authenticated)
                        {
                            throw new NucleusDataStoreException("Authentication of the connection failed for datastore " +
                                    dbName + " with user " + userName);
                        }
                    }
                }
                if (storeMgr.getBooleanProperty("datanucleus.readOnlyDatastore", false))
                {
                    ((DB)conn).setReadOnly(Boolean.TRUE);
                }
            }

            if (!startRequested)
            {
                // Start the "transaction"
                ((DB)conn).requestStart();
                startRequested = true;
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is starting");
            }
        }

        public void release()
        {
            if (commitOnRelease)
            {
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is committing");
                ((DB)conn).requestDone();
                startRequested = false;
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " committed connection");
            }
            super.release();
        }

        public void close()
        {
            if (conn == null)
            {
                return;
            }

            // Notify anything using this connection to use it now
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPreClose();
            }

            if (startRequested)
            {
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is committing");
                // End the current request
                ((DB)conn).requestDone();
                startRequested = false;
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " committed connection");
            }

            // Removes the connection from pooling
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
            }
        }

        public XAResource getXAResource()
        {
            if (xaRes == null)
            {
                if (conn == null)
                {
                    obtainNewConnection();
                }
                xaRes = new EmulatedXAResource(this, (DB)conn);
            }
            return xaRes;
        }
    }

    /**
     * Emulate the two phase protocol for non XA
     */
    static class EmulatedXAResource implements XAResource
    {
        ManagedConnectionImpl mconn;
        DB db;

        EmulatedXAResource(ManagedConnectionImpl mconn, DB db)
        {
            this.mconn = mconn;
            this.db = db;
        }

        public void start(Xid xid, int flags) throws XAException
        {
        }

        public void commit(Xid xid, boolean onePhase) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is committing for transaction "+xid.toString()+" with onePhase="+onePhase);
            db.requestDone();
            mconn.startRequested = false;
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " committed connection for transaction "+xid.toString()+" with onePhase="+onePhase);
        }

        public void rollback(Xid xid) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is rolling back for transaction "+xid.toString());
            db.requestDone();
            mconn.startRequested = false;
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " rolled back connection for transaction "+xid.toString());
        }

        public void end(Xid xid, int flags) throws XAException
        {
        }

        public void forget(Xid xid) throws XAException
        {
        }

        public int prepare(Xid xid) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is preparing for transaction "+xid.toString());
            return 0;
        }

        public Xid[] recover(int flags) throws XAException
        {
            throw new XAException("Unsupported operation");
        }

        public int getTransactionTimeout() throws XAException
        {
            return 0;
        }

        public boolean setTransactionTimeout(int timeout) throws XAException
        {
            return false;
        }

        public boolean isSameRM(XAResource xares) throws XAException
        {
            return (this == xares);
        }
    }
}