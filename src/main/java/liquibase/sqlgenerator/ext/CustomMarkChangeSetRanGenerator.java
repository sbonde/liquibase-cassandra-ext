package liquibase.sqlgenerator.ext;

import java.util.List;

import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.database.core.CassandraDatabase;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.MarkChangeSetRanGenerator;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.MarkChangeSetRanStatement;
import liquibase.statement.core.UpdateStatement;
import liquibase.util.LiquibaseUtil;
import liquibase.util.StringUtils;

/**
 * @author Sanjay Bonde
 */
public class CustomMarkChangeSetRanGenerator extends MarkChangeSetRanGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
	public Sql[] generateSql(MarkChangeSetRanStatement statement, Database database,
        SqlGeneratorChain sqlGeneratorChain) {
     	if(!(database instanceof CassandraDatabase)) {
    		return super.generateSql(statement, database, sqlGeneratorChain);
    	}
     	String dateValue = new java.sql.Date(System.currentTimeMillis()).toString();

        ChangeSet changeSet = statement.getChangeSet();

        SqlStatement runStatement;
        try {
        	CassandraDatabase cassandraDatabase = (CassandraDatabase)database;
            if (statement.getExecType().equals(ChangeSet.ExecType.FAILED) || statement.getExecType().equals(ChangeSet.ExecType.SKIPPED)) {
                return new Sql[0]; //don't mark
            } else  if (statement.getExecType().ranBefore) {
                runStatement = new UpdateStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
                    .addNewColumnValue("DATEEXECUTED", dateValue)
                    .addNewColumnValue("MD5SUM", escapeNull(changeSet.generateCheckSum().toString()))
                    .addNewColumnValue("EXECTYPE", escapeNull(statement.getExecType().value))
                    .setWhereClause("ID=? AND AUTHOR=? AND FILENAME=?")
                    .addWhereParameters(changeSet.getId(), changeSet.getAuthor(), changeSet.getFilePath());
            } else {
                runStatement = new InsertStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
                    .addColumnValue("ID", escapeNull(changeSet.getId()))
                    .addColumnValue("AUTHOR", escapeNull(changeSet.getAuthor()))
                    .addColumnValue("FILENAME", escapeNull(changeSet.getFilePath()))
                    .addColumnValue("DATEEXECUTED", dateValue)
                    .addColumnValue("ORDEREXECUTED", escapeNull(cassandraDatabase.getNextChangeSetSequenceValue()))
                    .addColumnValue("MD5SUM", escapeNull(changeSet.generateCheckSum().toString()))
                    .addColumnValue("DESCRIPTION", limitSize((String) escapeNull(changeSet.getDescription())))
                    .addColumnValue("COMMENTS", limitSize(StringUtils.trimToEmpty(changeSet.getComments())))
                    .addColumnValue("EXECTYPE", statement.getExecType().value)
                    .addColumnValue("LIQUIBASE", LiquibaseUtil.getBuildVersion().replaceAll("SNAPSHOT", "SNP"));

                String tag = null;
                List<Change> changes = changeSet.getChanges();
                if (changes != null && changes.size() == 1) {
                    Change change = changes.get(0);
                    if (change instanceof TagDatabaseChange) {
                        TagDatabaseChange tagChange = (TagDatabaseChange) change;
                        tag = tagChange.getTag();
                    }
                }
                if (tag != null) {
                    ((InsertStatement) runStatement).addColumnValue("TAG", tag);
                }
            }
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }

        return SqlGeneratorFactory.getInstance().generateSql(runStatement, database);
    }

    private Object escapeNull(Object value) {
        if (value == null) {
            return "'null'";
        }
        return value;
    }

    private String limitSize(String string) {
        int maxLength = 255;
        if (string.length() > maxLength) {
            return string.substring(0, maxLength - 3) + "...";
        }
        return string;
    }

}
