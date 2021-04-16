/*
 * ***** BEGIN LICENSE BLOCK *****

/*  Forsgren Enterprises DigiCOPS Collaboration Suite
    Oracle Database Data Store

    Copyright (C) 2021 Forsgren Enterprises LLC

    This class implements Oracle Database as the main data store for the CS.  
    Replacing open-source MariaDB/MySQL in Zimbra with an enterprise class database.

    Supports:  Oracle Multitenant (PDBs), Partitioning, Text, JSON Store
    Versions Supported/Tested:  Oracle 19c 19.10.0.0
*
 * Portions of this product are based on the
 *  Zimbra Collaboration Suite Server
 * Copyright (C) 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016 Synacor, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software Foundation,
 * version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 * ***** END LICENSE BLOCK *****
 */

package com.zimbra.cs.db;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.google.common.base.Joiner;
import com.zimbra.common.localconfig.LC;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.db.DbPool.DbConnection;

public class OracleDB extends Db {

    private Map<Db.Error, Integer> mErrorCodes;

    OracleDB() {
        mErrorCodes = new HashMap<Db.Error, Integer>(6);
        //Error Codes from Oracle Online Documentation, Database 19c
        mErrorCodes.put(Db.Error.DEADLOCK_DETECTED,        00060); // ORA-00060 Deadlock Detected
        mErrorCodes.put(Db.Error.DUPLICATE_ROW,            00001); // 
        mErrorCodes.put(Db.Error.FOREIGN_KEY_NO_PARENT,    2291); //
        mErrorCodes.put(Db.Error.FOREIGN_KEY_CHILD_EXISTS, 2292); //
        mErrorCodes.put(Db.Error.NO_SUCH_DATABASE,         39165); //
        mErrorCodes.put(Db.Error.NO_SUCH_TABLE,            942); //
        mErrorCodes.put(Db.Error.TABLE_FULL,               01653); //
    }

    // 3-16-2021 pcf removed MySQL-specific SQL statements, replaced with Oracle option on/off functionality
    @Override
    boolean supportsCapability(Db.Capability capability) {
        switch (capability) {
            case ORACLE_PARTITIONING_OPTION: return true;
            case ORACLE_MULTITENANT_OPTION:  return true;
            case ORACLE_RAC_OPTION:          return false;
            case ORACLE_ADVANCEDSEC_OPTION:  return false;
            case ORACLE_JSON_STORE:          return false;
            case MERGE_STATEMENT:            return true;
            case BITWISE_OPERATIONS:         return true;
            case BOOLEAN_DATATYPE:           return false;
            case CASE_SENSITIVE_COMPARISON:  return true;
            case CAST_AS_BIGINT:             return true;
            case CLOB_COMPARISON:            return true;
            case DISABLE_CONSTRAINT_CHECK:   return true;
            case NON_BMP_CHARACTERS:         return true;
            case ROW_LEVEL_LOCKING:          return true;
            case UNIQUE_NAME_INDEX:          return true;
            case DUMPSTER_TABLES:            return true;
        }
        return false;
    }

    @Override
    boolean compareError(SQLException e, Db.Error error) {
        Integer code = mErrorCodes.get(error);
        return (code != null && e.getErrorCode() == code);
    }

    @Override
    String forceIndexClause(String index) {
        // Oracle's optimizer will choose the right path, this is not needed
        return "";
    }

    @Override
    String getIFNULLClause(String expr1, String expr2) {
        return "NULLIF(" + expr1 + ", " + expr2 + ")";
    }

    @Override
    public String bitAND(String expr1, String expr2) {
        return expr1 + " & " + expr2;
    }

    @Override
    public String bitANDNOT(String expr1, String expr2) {
        return expr1 + " & ~" + expr2;
    }

    @Override
    DbPool.PoolConfig getPoolConfig() {
        return new OracleDBConfig();
    }

    @Override
    public boolean databaseExists(DbConnection conn, String dbname) throws ServiceException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int numSchemas = 0;

        // 4/12/2021 rewritten to use Oracle syntax
        try {
            stmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM DBA_USERS " +
                "WHERE USERNAME = ?");
            stmt.setString(1, dbname);
            rs = stmt.executeQuery();
            rs.next();
            numSchemas = rs.getInt(1);
        } catch (SQLException e) {
            throw ServiceException.FAILURE("Unable to determine whether database exists", e);
        } finally {
            DbPool.closeResults(rs);
            DbPool.closeStatement(stmt);
        }

        return (numSchemas > 0);
    }

    @Override
    public void enableStreaming(Statement stmt)
    throws SQLException {
        if (LC.jdbc_results_streaming_enabled.booleanValue()) {
            stmt.setFetchSize(Integer.MIN_VALUE);
        }
    }

    protected class OracleDBConfig extends DbPool.PoolConfig {
        OracleDBConfig() {
            mDriverClassName = getDriverClassName();
            mPoolSize = 100;
            mRootUrl = getRootUrl();
            mConnectionUrl = mRootUrl + "zimbra";
            mLoggerUrl = null;
            mSupportsStatsCallback = true;
            mDatabaseProperties = getDBProperties();

            // override pool size if specified in prefs
            String maxActive = (String) mDatabaseProperties.get("maxActive");
            if (maxActive != null) {
                try {
                    mPoolSize = Integer.parseInt(maxActive);
                } catch (NumberFormatException nfe) {
                    ZimbraLog.system.warn("exception parsing 'maxActive' pref; defaulting pool size to " + mPoolSize, nfe);
                }
            }
            ZimbraLog.misc.debug("Setting connection pool size to " + mPoolSize);
        }

        protected String getDriverClassName() {
            return "com.mysql.jdbc.Driver";
        }

        protected String getRootUrl() {
            return "jdbc:mysql://address=(protocol=tcp)(host=" + LC.mysql_bind_address.value() + ")(port=" + LC.mysql_port.value() + ")/";
        }

        protected Properties getDBProperties() {
            Properties props = new Properties();

            props.put("cacheResultSetMetadata", "true");
            props.put("cachePrepStmts", "true");
            // props.put("cacheCallableStmts", "true");
            props.put("prepStmtCacheSize", "25");
            // props.put("prepStmtCacheSqlLmiit", "256");
            props.put("autoReconnect", "true");
            props.put("useUnicode", "true");
            props.put("characterEncoding", "UTF-8");
            props.put("dumpQueriesOnException", "true");

            // props.put("connectTimeout", "0");    // connect timeout in msecs
            // props.put("initialTimeout", "2");    // time to wait between re-connects
            // props.put("maxReconnects", "3"");    // max number of reconnects to attempt

            // Set/override MySQL Connector/J connection properties from localconfig.
            // Localconfig keys with "zimbra_mysql_connector_" prefix are used.
            final String prefix = "zimbra_mysql_connector_";
            for (String key : LC.getAllKeys()) {
                if (!key.startsWith(prefix))
                    continue;
                String prop = key.substring(prefix.length());
                if (prop.length() > 0 && !prop.equalsIgnoreCase("logger")) {
                    props.put(prop, LC.get(key));
                    ZimbraLog.system.info("Setting mysql connector property: " + prop + "=" + LC.get(key));
                }
            }

            // These properties cannot be set with "zimbra_mysql_connector_" keys.
            props.put("user", LC.zimbra_mysql_user.value());
            props.put("password", LC.zimbra_mysql_password.value());

            return props;
        }
    }

    @Override
    public String toString() {
        return "Oracle Database 19c";
    }

    private final String sFlushCommand =
        "ALTER SYSTEM CHECKPOINT";
    

    @Override
    public synchronized void flushToDisk() {
        // Create a table and then drop it.  We take advantage of the fact that innodb will call
        // log_buffer_flush_to_disk() during CREATE TABLE or DELETE TABLE.
        DbConnection conn = null;
        PreparedStatement flushStmt = null;
        boolean success = false;
        try {
            try {
                conn = DbPool.getMaintenanceConnection();
                flushStmt = conn.prepareStatement(sFlushCommand);
                flushStmt.executeUpdate();
                success = true;
            } finally {
                DbPool.quietCloseStatement(flushStmt);
                if (conn != null)
                    conn.commit();
                DbPool.quietClose(conn);
            }
        } catch (SQLException e) {
            // If there's an error, let's just log it but not bubble up the exception.
            ZimbraLog.dbconn.warn("ignoring error while forcing Oracle to flush redo log to disk", e);
        } catch (ServiceException e) {
            // If there's an error, let's just log it but not bubble up the exception.
            ZimbraLog.dbconn.warn("ignoring error while forcing Oracle to flush redo log to disk", e);
        } finally {
            if (!success) {
                // There was an error.
                // The whole point of this method is to force Oracle to flush its log.  Oracle is
                // supposed to be flushing roughly every second normally, so let's simply
                // wait a few seconds to give the LGWR process a chance.
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {}
            }
        }
    }


    public static void main(String args[]) {
        // command line argument parsing
        Options options = new Options();
        CommandLine cl = Versions.parseCmdlineArgs(args, options);

        String outputDir = cl.getOptionValue("o");
        File outFile = new File(outputDir, "versions-init.sql");
        outFile.delete();

        try {
            String redoVer = com.zimbra.cs.redolog.Version.latest().toString();
            String outStr = "-- AUTO-GENERATED .SQL FILE - Generated by the Oracle versions tool\n" +
                    "INSERT INTO zimbra.config(name, value, description) VALUES\n" +
                    "\t('db.version', '" + Versions.DB_VERSION + "', 'db schema version'),\n" +
                    "\t('index.version', '" + Versions.INDEX_VERSION + "', 'index version'),\n" +
                    "\t('redolog.version', '" + redoVer + "', 'redolog version')\n" +
                    ";\nCOMMIT;\n";

            Writer output = new BufferedWriter(new FileWriter(outFile));
            output.write(outStr);
            if (output != null)
                output.close();
        } catch (IOException e){
            System.out.println("ERROR - caught exception at\n");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public String concat(String... fieldsToConcat) {
        Joiner joiner = Joiner.on(", ").skipNulls();
        return "CONCAT(" + joiner.join(fieldsToConcat) + ")";
    }

    @Override
    public String sign(String field) {
        return "SIGN(" + field + ")";
    }

    @Override
    public String lpad(String field, int padSize, String padString) {
        return "LPAD(" + field + ", " + padSize + ", '" + padString + "')";
    }

    @Override
    public String limit(int offset, int limit) {
        return "LIMIT " + offset + "," + limit;
    }
}