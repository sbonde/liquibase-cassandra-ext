package liquibase.changelog;

import liquibase.database.Database;
import liquibase.database.core.CassandraDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomChangeLogHistoryService extends StandardChangeLogHistoryService {

    private static final Logger log = LoggerFactory.getLogger(CustomChangeLogHistoryService.class);

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    /**
     * Customized method for Cassandra database
     * @return
     * @throws DatabaseException
     */
    @Override
    public boolean hasDatabaseChangeLogTable() throws DatabaseException {
        try {
        	Database database = getDatabase();
        	if(database instanceof CassandraDatabase) {
        		return hasTable(database, database.getDatabaseChangeLogTableName());
        	}
           return super.hasDatabaseChangeLogTable();
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }
    
    public void initCassandra() throws DatabaseException {
    	Database database = getDatabase();
    	Executor executor = ExecutorService.getInstance().getExecutor(database);
    	if(!hasTable(database, database.getDatabaseChangeLogTableName())) {
    		List<SqlStatement> statementsToExecute = new ArrayList<SqlStatement>();
    		SqlStatement createTableStatement = new CreateDatabaseChangeLogTableStatement();
            statementsToExecute.add(createTableStatement);
            log.info("Creating database history table with name: " + getDatabase().escapeTableName(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName()));
            for (SqlStatement sql : statementsToExecute) {
            	if (SqlGeneratorFactory.getInstance().supports(sql, database)) {
            		executor.execute(sql);
                    getDatabase().commit();
                 } else {
                	 log.info("Cannot run "+sql.getClass().getSimpleName()+" on "+getDatabase().getShortName()+" when checking databasechangelog table");
                 }
             }
    	 }
    }

    @Override
    public void init() throws DatabaseException {
        Database database = getDatabase();
        if(database instanceof CassandraDatabase) {
        	initCassandra();
        	return;
        }
        super.init();
    }

    /**
     * Returns the ChangeSets that have been run against the current getDatabase().
     */
    @Override
	public List<RanChangeSet> getRanChangeSets() throws DatabaseException {
        Database database = getDatabase();
    	List<RanChangeSet> ranChangeSetList = new ArrayList<RanChangeSet>();
        if(!(database instanceof CassandraDatabase)) {
        	return super.getRanChangeSets();
        }
        if (!hasDatabaseChangeLogTable()) {
        	return ranChangeSetList;
        }
        CassandraDatabase cassandraDatabase = (CassandraDatabase)database;
        List<CustomRanChangeSet> ranChangeSets = cassandraDatabase.getRanChangeSets(this, cassandraDatabase);
        List<RanChangeSet> result = new ArrayList<RanChangeSet>(0);
        for(CustomRanChangeSet ranChangeSet : ranChangeSets) {
        	RanChangeSet changeSet = new RanChangeSet(ranChangeSet.getChangeLog(), ranChangeSet.getId(), ranChangeSet.getAuthor(), ranChangeSet.getLastCheckSum(), ranChangeSet.getDateExecuted(), ranChangeSet.getTag(), ranChangeSet.getExecType(), ranChangeSet.getDescription(), ranChangeSet.getComments());
        	result.add(changeSet);
        }
        return result;
    }
    
    private boolean hasTable(Database database, String objectName) throws DatabaseException {
    	Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			String schema = database.getLiquibaseSchemaName();
				// This is a bit of hack to get at the underlying connection.
			JdbcConnection wrapperConnection = (JdbcConnection) database
					.getConnection();
			connection = wrapperConnection.getUnderlyingConnection();
			statement = connection.createStatement();

			String sql = "select \"keyspace_name\", \"columnfamily_name\" from \"system\".\"schema_columnfamilies\" where \"keyspace_name\" ='"+schema+"' and \"columnfamily_name\" ='"+objectName.toLowerCase()+"'";
			resultSet = statement.executeQuery(sql);
			if (!resultSet.next()) {
				return false;
			}
			return true;
		} catch (SQLException e) {
			throw new DatabaseException(e);
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}
    }
    
 }
