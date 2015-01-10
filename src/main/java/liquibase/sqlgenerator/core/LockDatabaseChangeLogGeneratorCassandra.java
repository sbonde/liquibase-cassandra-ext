package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;

public class LockDatabaseChangeLogGeneratorCassandra extends LockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
    
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return database instanceof CassandraDatabase;
    }
    
    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	if(!(database instanceof CassandraDatabase)) {
    		return super.generateSql(statement, database, sqlGeneratorChain);
    	}
    	String liquibaseSchema = database.getLiquibaseSchemaName();
        String liquibaseCatalog = database.getLiquibaseCatalogName();
        UpdateStatement updateStatement = new UpdateStatement(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogLockTableName());
        updateStatement.addNewColumnValue("LOCKED", true);
        updateStatement.addNewColumnValue("LOCKGRANTED",new java.util.Date());
        updateStatement.addNewColumnValue("LOCKEDBY", hostname + " (" + hostaddress + ")");
        updateStatement.setWhereClause(database.escapeColumnName(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogLockTableName(), "ID") + " = 1 ");
        return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);

    }
}