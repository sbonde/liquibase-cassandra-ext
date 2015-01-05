package liquibase.database.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import liquibase.change.CheckSum;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.CustomChangeLogHistoryService;
import liquibase.changelog.CustomRanChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.GetNextChangeSetSequenceValueStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;

/**
 * Cassandra 1.2.0 NoSQL database support.
 */
public class CassandraDatabase extends AbstractJdbcDatabase {
	public static final String PRODUCT_NAME = "Cassandra";
	private static Integer lastChangeSetSequenceValue = 0;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CassandraDatabase.class);

	// @Override
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
			LOGGER.info("No DATABASECHANGELOGLOCK available in cassandra.");
			hasChangeLogLockTable = false;
		} catch (ClassNotFoundException e) {
			LOGGER.error(e.getMessage(), e);
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
				LOGGER.info("No DATABASECHANGELOG available in cassandra.");
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
			LOGGER.info("No DATABASECHANGELOG available in cassandra.");
			hasChangeLogTable = false;
		} catch (ClassNotFoundException e) {
			LOGGER.error(e.getMessage(), e);
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
	// @Override
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
		// Check for DatabaseChangeLogLockTable
		// checkDatabaseChangeLogLockTable();
	}

	@Override
	public String getShortName() {
		return "cassandra";
	}

	public CassandraDatabase() {
		setDefaultSchemaName("");
	}

	@Override
	public int getPriority() {
		return PRIORITY_DEFAULT;
	}

	@Override
	protected String getDefaultDatabaseProductName() {
		return "Cassandra";
	}

	@Override
	public Integer getDefaultPort() {
		return 9160;
	}

	@Override
	public boolean supportsInitiallyDeferrableColumns() {
		return false;
	}

	@Override
	public boolean supportsSequences() {
		return false;
	}

	@Override
	public boolean isCorrectDatabaseImplementation(DatabaseConnection conn)
			throws DatabaseException {
		String databaseProductName = conn.getDatabaseProductName();
		return PRODUCT_NAME.equalsIgnoreCase(databaseProductName);
	}

	@Override
	public String getDefaultDriver(String url) {
		return "org.apache.cassandra.cql.jdbc.CassandraDriver";
	}

	@Override
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

	/*
	 * @Override public int getNextChangeSetSequenceValue() throws
	 * LiquibaseException { int next = 0; try { Statement statement =
	 * getStatement(); ResultSet rs = statement.executeQuery(
	 * "SELECT KEY, AUTHOR, ORDEREXECUTED FROM DATABASECHANGELOGLOCK"); while
	 * (rs.next()) { int order = rs.getInt("ORDEREXECUTED"); next =
	 * Math.max(order, next); } statement.close();
	 * 
	 * } catch (SQLException e) { } catch (ClassNotFoundException e) { } return
	 * next + 1; }
	 */
	// @Override
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

	public List<CustomRanChangeSet> getRanChangeSetsTemp(StandardChangeLogHistoryService changeLogHistoryService,
			Database database) throws DatabaseException {
		if (!(database instanceof CassandraDatabase)) {
			return null;
		}
		List<CustomRanChangeSet> ranChangeSetList = new ArrayList<CustomRanChangeSet>();
		if (changeLogHistoryService.hasDatabaseChangeLogTable()) {
			try {
				String fromTable = database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName());
				Statement statement = getStatement();
				ResultSet rs = statement
						.executeQuery("SELECT AUTHOR, COMMENTS, DATEEXECUTED, DESCRIPTION, EXECTYPE, FILENAME, ID, LIQUIBASE, MD5SUM, ORDEREXECUTED, TAG FROM "+fromTable);

				while (rs.next()) {
					/*
					 * String fileName = rs.getString("FILENAME"); String author
					 * = rs.getString("AUTHOR"); String id = rs.getString("ID");
					 * String md5sum = rs.getString("MD5SUM") == null ? null :
					 * rs.getString("MD5SUM"); String description =
					 * rs.getString("DESCRIPTION") == null ? null :
					 * rs.getString("DESCRIPTION"); Object tmpDateExecuted =
					 * rs.getString("DATEEXECUTED");
					 */
					String fileName = rs.getString(6);
					String author = rs.getString(1);
					String id = rs.getString(7);
					String md5sum = rs.getString(9) == null ? null : rs
							.getString(9);
					String description = rs.getString(4) == null ? null : rs
							.getString(4);
					Object tmpDateExecuted = rs.getString(3);

					Date dateExecuted = null;
					if (tmpDateExecuted instanceof Date) {
						dateExecuted = (Date) tmpDateExecuted;
					} else {
						DateFormat df = new SimpleDateFormat(
								"E MMM dd HH:mm:ss z yyyy");
						try {
							dateExecuted = df.parse((String) tmpDateExecuted);
						} catch (Exception e) {
							LOGGER.warn("Failed to parse date: "
									+ tmpDateExecuted + " expected "
									+ df.format(new Date(0)));
							dateExecuted = null;
						}
					}
					/*
					 * String tag = rs.getString("TAG") == null ? null :
					 * rs.getString("TAG"); String execType =
					 * rs.getString("EXECTYPE") == null ? null :
					 * rs.getString("EXECTYPE");
					 */
					String tag = rs.getString(11) == null ? null : rs
							.getString(11);
					String execType = rs.getString(5) == null ? null : rs
							.getString(5);
					String comments = "";
					int orderExecuted = 0;
					try {
						/*
						 * RanChangeSet ranChangeSet = new
						 * RanChangeSet(fileName, id, author,
						 * CheckSum.parse(md5sum), dateExecuted, tag,
						 * ChangeSet.ExecType.valueOf(execType), description);
						 */
						try {
							CustomRanChangeSet ranChangeSet = new CustomRanChangeSet(
									fileName, id, author, CheckSum.parse(md5sum),
									dateExecuted, tag,
									ChangeSet.ExecType.valueOf(execType), description,
									comments, (Integer) orderExecuted);
							LOGGER.debug("Changeset already ran on cassandra Cassandra: "
									+ ranChangeSet);
							ranChangeSetList.add(ranChangeSet);
						} catch (IllegalArgumentException e) {
							LOGGER.error("Unknown EXECTYPE from database: " + execType);
							throw e;
						}
					} catch (IllegalArgumentException e) {
						LOGGER.error("Unknown EXECTYPE from database: "
								+ execType);
						throw e;
					}
				}
				statement.close();
			} catch (Exception e) {
				throw new UnexpectedLiquibaseException(e);
			}
		}
		return ranChangeSetList;
	}

	@SuppressWarnings("rawtypes")
	public List<CustomRanChangeSet> getRanChangeSets(
			StandardChangeLogHistoryService changeLogHistoryService,
			Database database) throws DatabaseException {
		if (!(database instanceof CassandraDatabase)) {
			return null;
		}
		@SuppressWarnings("unused")
		String databaseChangeLogTableName = database.escapeTableName(
				database.getLiquibaseCatalogName(),
				database.getLiquibaseSchemaName(),
				database.getDatabaseChangeLogTableName());
		List<CustomRanChangeSet> ranChangeSetList = new ArrayList<CustomRanChangeSet>();
		if (changeLogHistoryService.hasDatabaseChangeLogTable()) {
			SqlStatement select = null;
			/*select = new SelectFromDatabaseChangeLogStatement("FILENAME",
					"AUTHOR", "ID", "MD5SUM", "DATEEXECUTED", "ORDEREXECUTED",
					"TAG", "EXECTYPE", "DESCRIPTION", "COMMENTS");*/
			select = new SelectFromDatabaseChangeLogStatement("FILENAME",
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
				//Object tmpDateExecuted = rs.get("DATEEXECUTED");
				Object orderExecuted = rs.get("ORDEREXECUTED");
				Date dateExecuted = null;
				/*if (tmpDateExecuted instanceof Date) {
					dateExecuted = (Date) tmpDateExecuted;
				} else {
					DateFormat df = new SimpleDateFormat(
							"E MMM dd HH:mm:ss z yyyy");
					try {
						dateExecuted = df.parse((String) tmpDateExecuted);
					} catch (ParseException e) {
						LOGGER.error(e.getMessage(), e);
					}
				}*/
				String tag = rs.get("TAG") == null ? null : rs.get("TAG")
						.toString();
				String execType = rs.get("EXECTYPE") == null ? null : rs.get(
						"EXECTYPE").toString();
				try {
					CustomRanChangeSet ranChangeSet = new CustomRanChangeSet(
							fileName, id, author, CheckSum.parse(md5sum),
							dateExecuted, tag,
							ChangeSet.ExecType.valueOf(execType), description,
							comments, (Integer) orderExecuted);
					ranChangeSetList.add(ranChangeSet);
				} catch (IllegalArgumentException e) {
					LOGGER.error("Unknown EXECTYPE from database: " + execType);
					throw e;
				}
			}
		}
		return ranChangeSetList;
	}

	public void clearChecksums() throws LiquibaseException {
		CustomChangeLogHistoryService changeLogHistoryService = (CustomChangeLogHistoryService) ChangeLogHistoryServiceFactory
				.getInstance().getChangeLogService(this);
		List<CustomRanChangeSet> ranChangeSets = getRanChangeSets(
				changeLogHistoryService, this);
		for (CustomRanChangeSet changeSet : ranChangeSets) {
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
		CustomChangeLogHistoryService changeLogHistoryService = (CustomChangeLogHistoryService) ChangeLogHistoryServiceFactory
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

		CustomRanChangeSet maxDateExecutedRanChangeSet = getMaxDateExecuted();
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

	private CustomRanChangeSet getMaxDateExecuted() throws Exception {
		CustomChangeLogHistoryService changeLogHistoryService = (CustomChangeLogHistoryService) ChangeLogHistoryServiceFactory
				.getInstance().getChangeLogService(this);
		List<CustomRanChangeSet> ranChangeSets = getRanChangeSets(
				changeLogHistoryService, this);
		if (ranChangeSets == null || ranChangeSets.isEmpty()) {
			return null;
		}
		CustomRanChangeSet maxDateExecutedRanChangeSet = ranChangeSets.get(0);
		for (CustomRanChangeSet changeSet : ranChangeSets) {
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