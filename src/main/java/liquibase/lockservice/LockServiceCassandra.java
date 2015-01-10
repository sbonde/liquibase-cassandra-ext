package liquibase.lockservice;

import liquibase.database.Database;
import liquibase.database.core.CassandraDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LockServiceCassandra extends StandardLockService {

    public LockServiceCassandra() {
    	super();
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof CassandraDatabase;
    }

    @Override
    public boolean hasDatabaseChangeLogLockTable() throws DatabaseException {
        boolean hasTable = false;
        try {
    		CassandraDatabase cassandraDatabase = (CassandraDatabase)database;
    		hasTable = cassandraDatabase.hasDatabaseChangeLogLockTable();
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
        return hasTable;
    }

    @Override
    public boolean acquireLock() throws LockException {
    	if(hasChangeLogLock()) {
    		return true;
    	}
      
    	Executor executor = ExecutorService.getInstance().getExecutor(database);

        try {
        	database.rollback();
            this.init();

            Boolean locked = ExecutorService.getInstance().getExecutor(database).queryForObject(new SelectFromDatabaseChangeLogLockStatement("LOCKED"), Boolean.class);

            if (locked) {
                return false;
            } else {

                executor.comment("Lock Database");
                int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());
                if (rowsUpdated > 1) {
                    throw new LockException("Did not update change log lock correctly");
                }
                //Cassandra UPDATE return no result (hence 0 default) for successful update.
                database.commit();
                LogFactory.getLogger().info("Successfully acquired change log lock");

                
                hasChangeLogLock = true;

                database.setCanCacheLiquibaseTableInfo(true);
                return true;
            }
        } catch (Exception e) {
            throw new LockException(e);
        } finally {
            try {
            	database.rollback();
            } catch (DatabaseException e) {
                ;
            }
        }

    }
    
    @Override
    public void releaseLock() throws LockException {
        Executor executor = ExecutorService.getInstance().getExecutor(database);
        try {
            if (this.hasDatabaseChangeLogLockTable()) {
                executor.comment("Release Database Lock");
                database.rollback();
                
                int updatedRows = executor.update(new UnlockDatabaseChangeLogStatement());
                
                //Cassandra CQL update doen't return row count updated, hence default 0. So fetch LOCKED rows
            	Boolean locked = ExecutorService.getInstance().getExecutor(database).queryForObject(new SelectFromDatabaseChangeLogLockStatement("LOCKED"), Boolean.class);
            	if(!locked) {
            		updatedRows = 1;
            	}
            	int expectedResult = 1;
                if (updatedRows != expectedResult) {
                    throw new LockException("Did not update change log lock correctly.\n\n" + updatedRows + " rows were updated instead of the expected 1 row using executor " + executor.getClass().getName()+" there are "+executor.queryForInt(new RawSqlStatement("select count(*) from "+database.getDatabaseChangeLogLockTableName()))+" rows in the table");
                }
                database.commit();
            }
        } catch (Exception e) {
            throw new LockException(e);
        } finally {
            try {
                hasChangeLogLock = false;

                database.setCanCacheLiquibaseTableInfo(false);

                LogFactory.getLogger().info("Successfully released change log lock");
                database.rollback();
            } catch (DatabaseException e) {
                ;
            }
        }
    }

    @SuppressWarnings("rawtypes")
	@Override
    public DatabaseChangeLogLock[] listLocks() throws LockException {
        try {
            if (!this.hasDatabaseChangeLogLockTable()) {
                return new DatabaseChangeLogLock[0];
            }

            List<DatabaseChangeLogLock> allLocks = new ArrayList<DatabaseChangeLogLock>();
            //SqlStatement sqlStatement = new SelectFromDatabaseChangeLogLockStatement("ID", "LOCKED", "LOCKGRANTED", "LOCKEDBY");
            SqlStatement sqlStatement = new SelectFromDatabaseChangeLogLockStatement("ID", "LOCKED", "LOCKEDBY");
            List<Map<String, ?>> rows = ExecutorService.getInstance().getExecutor(database).queryForList(sqlStatement);
            for (Map columnMap : rows) {
                Object lockedValue = columnMap.get("LOCKED");
                Boolean locked;
                if (lockedValue instanceof Number) {
                    locked = ((Number) lockedValue).intValue() == 1;
                } else {
                    locked = (Boolean) lockedValue;
                }
                if (locked != null && locked) {
                	Date lockGranted = null;
                	//Date lockGranted = (Date) columnMap.get("LOCKGRANTED");
                    allLocks.add(new DatabaseChangeLogLock(((Number) columnMap.get("ID")).intValue(), lockGranted, (String) columnMap.get("LOCKEDBY")));
                }
            }
            return allLocks.toArray(new DatabaseChangeLogLock[allLocks.size()]);
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

}
