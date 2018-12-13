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

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractEmulatedXAResource;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Implementation of a ConnectionFactory for MongoDB.
 * Accepts a URL of the form 
 * <pre>mongodb:[{server1}][/{dbName}][,{server2}[,{server3}]]</pre>
 * Defaults to a server of "localhost" if nothing specified.
 * Defaults to a DB name of "DataNucleus" if nothing specified.
 * Has a DB object per PM/EM. TODO Allow the option of having DB per PMF/EMF.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    public static final String MONGODB_CONNECT_TIMEOUT = "datanucleus.mongodb.connectTimeout";

    public static final String MONGODB_HEARTBEAT_CONNECT_TIMEOUT = "datanucleus.mongodb.heartbeatConnectTimeout";
    public static final String MONGODB_HEARTBEAT_FREQUENCY = "datanucleus.mongodb.heartbeatFrequency";
    public static final String MONGODB_HEARTBEAT_SOCKET_TIMEOUT = "datanucleus.mongodb.heartbeatSocketTimeout";

    public static final String MONGODB_MAX_CONNECTION_IDLE_TIME = "datanucleus.mongodb.maxConnectionIdleTime";
    public static final String MONGODB_MAX_CONNECTION_LIFE_TIME = "datanucleus.mongodb.maxConnectionLifeTime";
    public static final String MONGODB_MAX_WAIT_TIME = "datanucleus.mongodb.maxWaitTime";

    public static final String MONGODB_MIN_HEARTBEAT_FREQUENCY = "datanucleus.mongodb.minHeartbeatFrequency";
    public static final String MONGODB_MIN_CONNECTIONS_PER_HOST = "datanucleus.mongodb.minConnectionsPerHost";

    public static final String MONGODB_SERVER_SELECTION_TIMEOUT = "datanucleus.mongodb.serverSelectionTimeout";

    public static final String MONGODB_SOCKET_TIMEOUT = "datanucleus.mongodb.socketTimeout";

    public static final String MONGODB_SSL_ENABLED = "datanucleus.mongodb.sslEnabled";
    public static final String MONGODB_SSL_INVALID_HOSTNAME_ALLOWED = "datanucleus.mongodb.sslInvalidHostnameAllowed";

    public static final String MONGODB_CONNECTIONS_PER_HOST = "datanucleus.mongodb.connectionsPerHost";
    public static final String MONGODB_THREAD_BLOCK_FOR_MULTIPLIER = "datanucleus.mongodb.threadsAllowedToBlockForConnectionMultiplier";
    public static final String MONGODB_REPLICA_SET_NAME = "datanucleus.mongodb.replicaSetName";

    String dbName = "DataNucleus";

    MongoClient mongo;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // "mongodb:[server]/database"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "' (or alias)");
        }

        String remains = url.substring(7).trim();
        if (remains.indexOf(':') == 0)
        {
            remains = remains.substring(1);
        }

        // Split into any replica sets
        try
        {
            List<ServerAddress> serverAddrs = new ArrayList<>();
            if (remains.length() == 0)
            {
                // "mongodb:"
                serverAddrs.add(new ServerAddress());
            }
            else
            {
                StringTokenizer tokeniser = new StringTokenizer(remains, ",");
                while (tokeniser.hasMoreTokens())
                {
                    String token = tokeniser.nextToken();

                    String serverName = "localhost";
                    if (serverAddrs.isEmpty())
                    {
                        // Set dbName
                        int dbNameSepPos = token.indexOf("/");
                        if (dbNameSepPos >= 0)
                        {
                            if (dbNameSepPos < token.length())
                            {
                                String dbNameStr = token.substring(dbNameSepPos + 1);
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
                        addr = new ServerAddress(serverName.substring(0, portSeparatorPos), Integer.valueOf(serverName.substring(portSeparatorPos + 1)).intValue());
                    }
                    else
                    {
                        addr = new ServerAddress(serverName);
                    }
                    serverAddrs.add(addr);
                }
            }

            MongoCredential credential = null;
            String userName = storeMgr.getConnectionUserName();
            String password = storeMgr.getConnectionPassword();
            if (!StringUtils.isWhitespace(userName))
            {
                credential = MongoCredential.createCredential(userName, dbName, password.toCharArray());
            }

            // Create the Mongo connection pool
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("MongoDB.ServerConnect", dbName, serverAddrs.size(), StringUtils.collectionToString(serverAddrs)));
            }

            if (serverAddrs.size() == 1)
            {
                if (credential == null)
                {
                    mongo = new MongoClient(serverAddrs.get(0), getMongodbOptions(storeMgr));
                }
                else
                {
                    mongo = new MongoClient(serverAddrs.get(0), credential, getMongodbOptions(storeMgr));
                }
            }
            else
            {
                if (credential == null)
                {
                    mongo = new MongoClient(serverAddrs, getMongodbOptions(storeMgr));
                }
                else
                {
                    mongo = new MongoClient(serverAddrs, credential, getMongodbOptions(storeMgr));
                }
            }
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug("Created MongoClient object on resource " + getResourceName());
            }
        }
        catch (MongoException me)
        {
            throw new NucleusDataStoreException("Unable to connect to MongoClient", me);
        }
        catch (Exception e)
        {
            // We catch this since with MongoDB 2.x driver ServerAddress(...) can throw UnknownHostException
            throw new NucleusDataStoreException("Unable to connect to MongoClient", e);
        }
    }

    private MongoClientOptions getMongodbOptions(StoreManager storeManager)
    {
        MongoClientOptions.Builder mongoOptionsBuilder = MongoClientOptions.builder();
        if (storeManager.hasProperty(MONGODB_CONNECTIONS_PER_HOST))
        {
            mongoOptionsBuilder.connectionsPerHost(storeManager.getIntProperty(MONGODB_CONNECTIONS_PER_HOST));
        }
        if (storeManager.hasProperty(MONGODB_CONNECT_TIMEOUT))
        {
            mongoOptionsBuilder.connectTimeout(storeManager.getIntProperty(MONGODB_CONNECT_TIMEOUT));
        }

        if (storeManager.hasProperty(MONGODB_HEARTBEAT_CONNECT_TIMEOUT))
        {
            mongoOptionsBuilder.heartbeatConnectTimeout(storeManager.getIntProperty(MONGODB_HEARTBEAT_CONNECT_TIMEOUT));
        }
        if (storeManager.hasProperty(MONGODB_HEARTBEAT_FREQUENCY))
        {
            mongoOptionsBuilder.heartbeatFrequency(storeManager.getIntProperty(MONGODB_HEARTBEAT_FREQUENCY));
        }
        if (storeManager.hasProperty(MONGODB_HEARTBEAT_SOCKET_TIMEOUT))
        {
            mongoOptionsBuilder.heartbeatSocketTimeout(storeManager.getIntProperty(MONGODB_HEARTBEAT_SOCKET_TIMEOUT));
        }

        if (storeManager.hasProperty(MONGODB_MAX_CONNECTION_IDLE_TIME))
        {
            mongoOptionsBuilder.maxConnectionIdleTime(storeManager.getIntProperty(MONGODB_MAX_CONNECTION_IDLE_TIME));
        }
        if (storeManager.hasProperty(MONGODB_MAX_CONNECTION_LIFE_TIME))
        {
            mongoOptionsBuilder.maxConnectionLifeTime(storeManager.getIntProperty(MONGODB_MAX_CONNECTION_LIFE_TIME));
        }
        if (storeManager.hasProperty(MONGODB_MAX_WAIT_TIME))
        {
            mongoOptionsBuilder.maxWaitTime(storeManager.getIntProperty(MONGODB_MAX_WAIT_TIME));
        }

        if (storeManager.hasProperty(MONGODB_MIN_CONNECTIONS_PER_HOST))
        {
            mongoOptionsBuilder.minConnectionsPerHost(storeManager.getIntProperty(MONGODB_MIN_CONNECTIONS_PER_HOST));
        }
        if (storeManager.hasProperty(MONGODB_MIN_HEARTBEAT_FREQUENCY))
        {
            mongoOptionsBuilder.minHeartbeatFrequency(storeManager.getIntProperty(MONGODB_MIN_HEARTBEAT_FREQUENCY));
        }

        if (storeManager.hasProperty(MONGODB_SERVER_SELECTION_TIMEOUT))
        {
            mongoOptionsBuilder.serverSelectionTimeout(storeManager.getIntProperty(MONGODB_SERVER_SELECTION_TIMEOUT));
        }

        if (storeManager.hasProperty(MONGODB_SOCKET_TIMEOUT))
        {
            mongoOptionsBuilder.socketTimeout(storeManager.getIntProperty(MONGODB_SOCKET_TIMEOUT));
        }

        if (storeManager.hasProperty(MONGODB_SSL_ENABLED))
        {
            mongoOptionsBuilder.sslEnabled(storeManager.getBooleanProperty(MONGODB_SSL_ENABLED));
        }
        if (storeManager.hasProperty(MONGODB_SSL_INVALID_HOSTNAME_ALLOWED))
        {
            mongoOptionsBuilder.sslInvalidHostNameAllowed(storeManager.getBooleanProperty(MONGODB_SSL_INVALID_HOSTNAME_ALLOWED));
        }

        if (storeManager.hasProperty(MONGODB_THREAD_BLOCK_FOR_MULTIPLIER))
        {
            mongoOptionsBuilder.threadsAllowedToBlockForConnectionMultiplier(storeManager.getIntProperty(MONGODB_THREAD_BLOCK_FOR_MULTIPLIER));
        }

        if (storeManager.hasProperty(MONGODB_REPLICA_SET_NAME))
        {
            mongoOptionsBuilder.requiredReplicaSetName(storeManager.getStringProperty(MONGODB_REPLICA_SET_NAME));
        }

        return mongoOptionsBuilder.build();
    }

    public void close()
    {
        if (NucleusLogger.CONNECTION.isDebugEnabled())
        {
            NucleusLogger.CONNECTION.debug("Closing MongoClient object on resource " + getResourceName());
        }
        mongo.close();
        super.close();
    }

    /**
     * Obtain a connection from the Factory. 
     * The connection will be enlisted within the transaction associated to the ExecutionContext.
     * Note that MongoDB doesn't have a "transaction" as such, so commit/rollback don't apply.
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param options Options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        return new ManagedConnectionImpl(ec);
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        ExecutionContext ec;

        XAResource xaRes = null;

        public ManagedConnectionImpl(ExecutionContext ec)
        {
            this.ec = ec;
        }

        /*
         * (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#closeAfterTransactionEnd()
         */
        @Override
        public boolean closeAfterTransactionEnd()
        {
            // Don't call close() immediately after transaction commit/rollback/end since we want to hang on to the connection until the ExecutionContext ends
            return false;
        }

        public Object getConnection()
        {
            if (conn == null)
            {
                obtainNewConnection();
            }
            return conn;
        }

        @SuppressWarnings("deprecation")
        protected void obtainNewConnection()
        {
            if (conn == null)
            {
                // Create new connection
                conn = mongo.getDB(dbName); // TODO Change this to getDatabase(...)
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(Localiser.msg("009011", this.toString(), getResourceName()));
                }
            }
        }

        public void release()
        {
            if (commitOnRelease)
            {
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(Localiser.msg("009015", this.toString()));
                }
            }
            super.release();
        }

        public void close()
        {
            // Notify anything using this connection to use it now
            for (ManagedConnectionResourceListener listener : listeners)
            {
                listener.managedConnectionPreClose();
            }

            if (conn != null)
            {
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(Localiser.msg("009013", this.toString()));
                }
            }

            for (int i=0;i<listeners.size();i++)
            {
                listeners.get(i).managedConnectionPostClose();
            }

            this.ec = null;
            this.xaRes = null;

            super.close();
        }

        public XAResource getXAResource()
        {
            if (xaRes == null)
            {
                if (conn == null)
                {
                    obtainNewConnection();
                }
                xaRes = new EmulatedXAResource(this, (DB) conn);
            }
            return xaRes;
        }
    }

    /**
     * Emulate the two phase protocol for non XA
     */
    static class EmulatedXAResource extends AbstractEmulatedXAResource
    {
        DB db;

        EmulatedXAResource(ManagedConnectionImpl mconn, DB db)
        {
            super(mconn);
            this.db = db;
        }

        public void commit(Xid xid, boolean onePhase) throws XAException
        {
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("009015", this.toString()));
            }
            super.commit(xid, onePhase);
        }

        public void rollback(Xid xid) throws XAException
        {
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug(Localiser.msg("009016", this.toString()));
            }
            super.rollback(xid);
        }
    }
}