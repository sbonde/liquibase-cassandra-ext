package liquibase.database.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import liquibase.change.CheckSum;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.ChangeLogHistoryServiceCassandra;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.GetNextChangeSetSequenceValueStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;

/**
 * Cassandra NoSQL database support.
 */
public class CassandraDatabase extends AbstractJdbcDatabase {
	public static final String PRODUCT_NAME = "Cassandra";
	private static Integer lastChangeSetSequenceValue = 0;

	public boolean hasDatabaseChangeLogLockTable() throws DatabaseException {
		boolean hasChangeLogLockTable;
		try {
			String liquibaseSchema = this.getLiquibaseSchemaName();
			Statement statement = getStatement();
			String escapeTableName = escapeTableName(getLiquibaseCatalogName(),
					liquibaseSchema, getDatabaseChangeLogLockTableName());
			statement.executeQuery("select ID from " + escapeTableName);
			statement.close();
			hasChangeLogLockTable = true;
		} catch (SQLException e) {
			LogFactory.getLogger().info("No DATABASECHANGELOGLOCK available in cassandra.");
			hasChangeLogLockTable = false;
		} catch (ClassNotFoundException e) {
			LogFactory.getLogger().info(e.getMessage());
			hasChangeLogLockTable = false;
		}

		// needs to be generated up front
		return hasChangeLogLockTable;
	}

	/**
	 * This method will check the database ChangeLogLock table used to keep
	 * track of if a machine is updating the database. If the table does not
	 * exist it will create one otherwise it will not do anything besides
	 * outputting a log message.
	 */
	public void checkDatabaseChangeLogLockTable() throws DatabaseException {
		if (!hasDatabaseChangeLogLockTable()) {
			try {
				Statement statement = getStatement();
				statement
						.executeUpdate("CREATE TABLE DATABASECHANGELOGLOCK (ID int PRIMARY KEY, LOCKED boolean, LOCKGRANTED timestamp, LOCKEDBY text)");
				statement.close();

				statement = getStatement();
				statement
						.executeUpdate("insert into DATABASECHANGELOGLOCK (ID, LOCKED) values (1, false)");
				statement.close();
			} catch (SQLException e) {
				LogFactory.getLogger().info("No DATABASECHANGELOG available in cassandra.");
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	// @Override
	public boolean hasDatabaseChangeLogTable() throws DatabaseException {
		boolean hasChangeLogTable;
		try {
			Statement statement = getStatement();
			statement.executeQuery("select ID from DATABASECHANGELOG");
			statement.close();
			hasChangeLogTable = true;
		} catch (SQLException e) {
			LogFactory.getLogger().info("No DATABASECHANGELOG available in cassandra.");
			hasChangeLogTable = false;
		} catch (ClassNotFoundException e) {
			LogFactory.getLogger(e.getMessage());
			hasChangeLogTable = false;
		}

		// needs to be generated up front
		return hasChangeLogTable;
	}

	/**
	 * This method will check the database ChangeLog table used to keep track of
	 * the changes in the file. If the table does not exist it will create one
	 * otherwise it will not do anything besides outputting a log message.
	 * 
	 * @param updateExistingNullChecksums
	 * @param contexts
	 */
	public void checkDatabaseChangeLogTable(
			boolean updateExistingNullChecksums,
			DatabaseChangeLog databaseChangeLog, String... contexts)
			throws DatabaseException {
		if (!hasDatabaseChangeLogTable()) {
			try {
				Statement statement = getStatement();
				statement
						.executeUpdate("CREATE TABLE DATABASECHANGELOG (ID text PRIMARY KEY, AUTHOR text, FILENAME text, DATEEXECUTED timestamp, ORDEREXECUTED int, EXECTYPE text, MD5SUM text, DESCRIPTION text, COMMENTS text, TAG text, LIQUIBASE text)");
				statement.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public String getShortName() {
		return "cassandra";
	}

	public CassandraDatabase() {
		setDefaultSchemaName("");
	}

	public int getPriority() {
		return PRIORITY_DEFAULT;
	}

	@Override
	protected String getDefaultDatabaseProductName() {
		return "Cassandra";
	}

	public Integer getDefaultPort() {
		return 9160;
	}

	public boolean supportsInitiallyDeferrableColumns() {
		return false;
	}

	@Override
	public boolean supportsSequences() {
		return false;
	}

	public boolean isCorrectDatabaseImplementation(DatabaseConnection conn)
			throws DatabaseException {
		String databaseProductName = conn.getDatabaseProductName();
		return PRODUCT_NAME.equalsIgnoreCase(databaseProductName);
	}

	public String getDefaultDriver(String url) {
		return "org.apache.cassandra.cql.jdbc.CassandraDriver";
	}

	public boolean supportsTablespaces() {
		return false;
	}

	@Override
	public boolean supportsRestrictForeignKeys() {
		return false;
	}

	@Override
	public boolean supportsDropTableCascadeConstraints() {
		return false;
	}

	@Override
	public boolean isAutoCommit() throws DatabaseException {
		return true;
	}

	@Override
	public void setAutoCommit(boolean b) throws DatabaseException {
	}

	@Override
	public boolean isCaseSensitive() {
		return true;
	}

	public int getNextChangeSetSequenceValue() throws LiquibaseException {
		if (lastChangeSetSequenceValue == null) {
			if (getConnection() == null) {
				lastChangeSetSequenceValue = 0;
			} else {
				lastChangeSetSequenceValue = ExecutorService
						.getInstance()
						.getExecutor(this)
						.queryForInt(
								new GetNextChangeSetSequenceValueStatement());
			}
		}

		return ++lastChangeSetSequenceValue;
	}

	protected Statement getStatement() throws ClassNotFoundException,
			SQLException {
		String url = super.getConnection().getURL();
		Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		Connection con = DriverManager.getConnection(url);
		Statement statement = con.createStatement();
		return statement;
	}

	@SuppressWarnings("rawtypes")
	public List<RanChangeSet> getRanChangeSets(
			StandardChangeLogHistoryService changeLogHistoryService,
			Database database) throws DatabaseException {
		@SuppressWarnings("unused")
		String databaseChangeLogTableName = database.escapeTableName(
				database.getLiquibaseCatalogName(),
				database.getLiquibaseSchemaName(),
				database.getDatabaseChangeLogTableName());
		List<RanChangeSet> ranChangeSetList = new ArrayList<RanChangeSet>();
		if (changeLogHistoryService.hasDatabaseChangeLogTable()) {
			SqlStatement select = new SelectFromDatabaseChangeLogStatement("FILENAME",
			 "AUTHOR", "ID", "MD5SUM", "ORDEREXECUTED", "TAG", "EXECTYPE",
			 "DESCRIPTION", "COMMENTS");
			List<Map<String, ?>> results = ExecutorService.getInstance()
					.getExecutor(database).queryForList(select);
			for (Map rs : results) {
				String fileName = rs.get("FILENAME").toString();
				String author = rs.get("AUTHOR").toString();
				String id = rs.get("ID").toString();
				String md5sum = rs.get("MD5SUM") == null ? null : rs.get(
						"MD5SUM").toString();
				String description = rs.get("DESCRIPTION") == null ? null : rs
						.get("DESCRIPTION").toString();
				String comments = rs.get("COMMENTS") == null ? null : rs.get(
						"COMMENTS").toString();
				Object tmpDateExecuted = rs.get("DATEEXECUTED");
				Object orderExecuted = rs.get("ORDEREXECUTED");
				Date dateExecuted = null;
				if (tmpDateExecuted instanceof Date) {
					dateExecuted = (Date) tmpDateExecuted;
				} else if(tmpDateExecuted != null){
					DateFormat df = new SimpleDateFormat(
							"E MMM dd HH:mm:ss z yyyy");
					try {
						dateExecuted = df.parse((String) tmpDateExecuted);
					} catch (ParseException e) {
						LogFactory.getLogger().info(e.getMessage());
					}
				}
				String tag = rs.get("TAG") == null ? null : rs.get("TAG")
						.toString();
				String execType = rs.get("EXECTYPE") == null ? null : rs.get(
						"EXECTYPE").toString();
				try {
					RanChangeSet ranChangeSet = new RanChangeSet(
							fileName, id, author, CheckSum.parse(md5sum),
							dateExecuted, tag,
							ChangeSet.ExecType.valueOf(execType), description,
							comments);
					ranChangeSet.setOrderExecuted((Integer) orderExecuted);
					ranChangeSetList.add(ranChangeSet);
				} catch (IllegalArgumentException e) {
					LogFactory.getLogger().info("Unknown EXECTYPE from database: " + execType);
					throw e;
				}
			}
		}
		return ranChangeSetList;
	}

	public void clearChecksums() throws LiquibaseException {
		List<RanChangeSet> ranChangeSets = this.getRanChangeSetList();
		for (RanChangeSet changeSet : ranChangeSets) {
			UpdateStatement updateStatement = new UpdateStatement(
					getLiquibaseCatalogName(), getLiquibaseSchemaName(),
					getDatabaseChangeLogTableName());
			updateStatement.addNewColumnValue("MD5SUM", null);
			updateStatement.setWhereClause(escapeColumnName(
					getLiquibaseCatalogName(), getLiquibaseSchemaName(),
					getDatabaseChangeLogTableName(), "ID")
					+ " = '"
					+ changeSet.getId()
					+ "' AND "
					+ escapeColumnName(getLiquibaseCatalogName(),
							getLiquibaseSchemaName(),
							getDatabaseChangeLogTableName(), "DATEEXECUTED")
					+ " = "
					+ changeSet.getDateExecuted().getTime()
					+ " AND "
					+ escapeColumnName(getLiquibaseCatalogName(),
							getLiquibaseSchemaName(),
							getDatabaseChangeLogTableName(), "ORDEREXECUTED")
					+ " = " + changeSet.getOrderExecuted());
			ExecutorService.getInstance().getExecutor(this)
					.execute(updateStatement);
		}
		commit();
	}

	@Override
	public void tag(final String tagString) throws DatabaseException {
		Executor executor = ExecutorService.getInstance().getExecutor(this);
		ChangeLogHistoryServiceCassandra changeLogHistoryService = (ChangeLogHistoryServiceCassandra) ChangeLogHistoryServiceFactory
				.getInstance().getChangeLogService(this);

		try {
			int totalRows = ExecutorService
					.getInstance()
					.getExecutor(this)
					.queryForInt(
							new SelectFromDatabaseChangeLogStatement("COUNT(*)"));
			if (totalRows == 0) {
				ChangeSet emptyChangeSet = new ChangeSet(
						String.valueOf(new Date().getTime()), "liquibase",
						false, false, "liquibase-internal", null, null,
						getObjectQuotingStrategy(), null);
				changeLogHistoryService.setExecType(emptyChangeSet,
						ChangeSet.ExecType.EXECUTED);
			}

			// Timestamp lastExecutedDate = (Timestamp)
			// this.getExecutor().queryForObject(createChangeToTagSQL(),
			// Timestamp.class);
			executor.execute(generateTagStatement(tagString));
			commit();

			// getRanChangeSets().get(getRanChangeSets().size() -
			// 1).setTag(tagString);
			getRanChangeSetList().get(getRanChangeSetList().size() - 1).setTag(
					tagString);
		} catch (Exception e) {
			throw new DatabaseException(e);
		}
	}

	private SqlStatement generateTagStatement(String tagString)
			throws Exception {
		String liquibaseSchema = null;
		liquibaseSchema = getLiquibaseSchemaName();
		UpdateStatement updateStatement = new UpdateStatement(
				getLiquibaseCatalogName(), liquibaseSchema,
				getDatabaseChangeLogTableName());
		updateStatement.addNewColumnValue("TAG", tagString);

		RanChangeSet maxDateExecutedRanChangeSet = getMaxDateExecuted();
		if (maxDateExecutedRanChangeSet == null) {
			throw new LiquibaseException("No change sets found");
		}
		updateStatement.setWhereClause(escapeColumnName(
				getLiquibaseCatalogName(), getLiquibaseSchemaName(),
				getDatabaseChangeLogTableName(), "ID")
				+ " = '"
				+ maxDateExecutedRanChangeSet.getId()
				+ "' AND "
				+ escapeColumnName(getLiquibaseCatalogName(),
						getLiquibaseSchemaName(),
						getDatabaseChangeLogTableName(), "DATEEXECUTED")
				+ " = "
				+ maxDateExecutedRanChangeSet.getDateExecuted().getTime()
				+ " AND "
				+ escapeColumnName(getLiquibaseCatalogName(),
						getLiquibaseSchemaName(),
						getDatabaseChangeLogTableName(), "ORDEREXECUTED")
				+ " = " + maxDateExecutedRanChangeSet.getOrderExecuted());
		return updateStatement;
	}

	private RanChangeSet getMaxDateExecuted() throws Exception {
		/*CustomChangeLogHistoryService changeLogHistoryService = (CustomChangeLogHistoryService) ChangeLogHistoryServiceFactory
				.getInstance().getChangeLogService(this);
		List<RanChangeSet> ranChangeSets = getRanChangeSets(
				changeLogHistoryService, this);*/
		List<RanChangeSet> ranChangeSets = this.getRanChangeSetList();
		if (ranChangeSets == null || ranChangeSets.isEmpty()) {
			return null;
		}
		RanChangeSet maxDateExecutedRanChangeSet = ranChangeSets.get(0);
		for (RanChangeSet changeSet : ranChangeSets) {
			if (changeSet.getDateExecuted().after(
					maxDateExecutedRanChangeSet.getDateExecuted())) {
				maxDateExecutedRanChangeSet = changeSet;
			}
		}
		return maxDateExecutedRanChangeSet;
	}

	@Override
	public String getCurrentDateTimeFunction() {
		// no alternative in cassandra, using client time
		return String.valueOf(System.currentTimeMillis());
	}

	@Override
	public String getDatabaseChangeLogTableName() {
		return super.getDatabaseChangeLogTableName().toLowerCase();
	}

	/**
	 * @see liquibase.database.Database#getDatabaseChangeLogLockTableName()
	 */
	@Override
	public String getDatabaseChangeLogLockTableName() {
		return super.getDatabaseChangeLogLockTableName().toLowerCase();
	}

}