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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ChangeLogHistoryService for Cassandra database
 * @author sbonde
 *
 */
public class ChangeLogHistoryServiceCassandra extends StandardChangeLogHistoryService {

    private static final Logger log = LoggerFactory.getLogger(ChangeLogHistoryServiceCassandra.class);

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof CassandraDatabase;
    }
  
    @Override
    public boolean hasDatabaseChangeLogTable() throws DatabaseException {
        try {
        	Database database = getDatabase();
        	return hasTable(database, database.getDatabaseChangeLogTableName());
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }
    
    public void initialzeChangeLogTable() throws DatabaseException {
    	Database database = getDatabase();
    	Executor executor = ExecutorService.getInstance().getExecutor(database);
    	if(!hasTable(database, database.getDatabaseChangeLogTableName())) {
    		SqlStatement createTableStatement = new CreateDatabaseChangeLogTableStatement();
    		if (SqlGeneratorFactory.getInstance().supports(createTableStatement, database)) {
        		executor.execute(createTableStatement);
                getDatabase().commit();
             } else {
            	 log.info("Cannot run "+createTableStatement.getClass().getSimpleName()+" on "+getDatabase().getShortName()+" when checking databasechangelog table");
             }
    	 }
    }

    @Override
    public void init() throws DatabaseException {
    	initialzeChangeLogTable();
    }

    @Override
	public List<RanChangeSet> getRanChangeSets() throws DatabaseException {
        Database database = getDatabase();
        CassandraDatabase cassandraDatabase = (CassandraDatabase)database;
        return cassandraDatabase.getRanChangeSets(this, cassandraDatabase);
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
