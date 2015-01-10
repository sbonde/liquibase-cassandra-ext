package liquibase;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.URL;

import org.junit.Ignore;
import org.junit.Test;

import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.core.CassandraDatabase;
import liquibase.lockservice.LockServiceCassandra;
import liquibase.lockservice.LockServiceFactory;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.util.LiquibaseExtensionUtil;

/**
 * @author Sanjay Bonde
 */
@Ignore
public class CassandraExtensionsTest {

	@Test
	public void runLiquibase() throws Exception {
		Database database = LiquibaseExtensionUtil.createCassandraDatabase("localhost","9160","dbuser");
		assertNotNull(database.getConnection());

		File basedir = new File(System.getProperty("user.dir"));

		FileSystemResourceAccessor resourceAccessor = new FileSystemResourceAccessor(
				basedir.getAbsolutePath());

		System.out.println("Executing presys on dbuser");
		URL url = getClass().getResource("/cassandra/presys-changelog.xml");
		File changeLog = new File(url.toURI());
		
		Liquibase liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		String contexts = "";
		try {
		liquibase.update(contexts);
		}catch(Throwable ex) {
			ex.printStackTrace();
		}
		
		//
		System.out.println("Executing changelog on abc");
		database = LiquibaseExtensionUtil.createCassandraDatabase("localhost","9160","abc");
		assertNotNull(database.getConnection());

		url = getClass().getResource("/cassandra/changelog.xml");
		changeLog = new File(url.toURI());
		
		liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		liquibase.update(contexts);
		System.out.println("Executing pre-adhoc-changelog on abc");
		url = getClass().getResource("/cassandra/pre-adhoc-changelog.xml");
		changeLog = new File(url.toURI());
		
		liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		liquibase.update(contexts);
		System.out.println("Executing post-adhoc-changelog on abc");
		url = getClass().getResource("/cassandra/post-adhoc-changelog.xml");
		changeLog = new File(url.toURI());
		
		liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		liquibase.update(contexts);
		System.out.println("Finished");
	}
	
	@Ignore
	@Test
	public void runLiquibaseTag() throws Exception {
		LockServiceFactory.getInstance().register(new LockServiceCassandra());
		Database database = LiquibaseExtensionUtil.createCassandraDatabase("localhost","9160","dbaasuser");
		assertNotNull(database.getConnection());

		File basedir = new File(System.getProperty("user.dir"));

		FileSystemResourceAccessor resourceAccessor = new FileSystemResourceAccessor(
				basedir.getAbsolutePath());

		URL url = getClass().getResource("/cassandra/presys-changelog.xml");
		File changeLog = new File(url.toURI());
		
		Liquibase liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		liquibase.tag("tagged");
	}
	
	@Ignore
	@Test
	public void update() throws Exception {
		LockServiceFactory.getInstance().register(new LockServiceCassandra());
		String host = "localhost";
		
		Database database = LiquibaseExtensionUtil.createCassandraDatabase(host, "9160", "dbaasuser");
		assertNotNull(database.getConnection());

		File basedir = new File(System.getProperty("user.dir"));

		FileSystemResourceAccessor resourceAccessor = new FileSystemResourceAccessor(
				basedir.getAbsolutePath());

		URL url = getClass().getResource("/cassandra/global/presys-changelog.xml");
		File changeLog = new File(url.toURI());
		
		Liquibase liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		String contexts = "";
		liquibase.update(contexts);

		database = LiquibaseExtensionUtil.createCassandraDatabase(host, "9160", "global_store_orders");
		assertNotNull(database.getConnection());

		url = getClass().getResource("/cassandra/global/core-changelog.xml");
		changeLog = new File(url.toURI());
		
		liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		liquibase.update(contexts);
	}
	
	@Ignore
	@Test
	public void clearCheckSums() throws Exception {
		LockServiceFactory.getInstance().register(new LockServiceCassandra());
		Database database = LiquibaseExtensionUtil.createCassandraDatabase("localhost", "9160", "global_store_orders");
		assertNotNull(database.getConnection());

		File basedir = new File(System.getProperty("user.dir"));

		FileSystemResourceAccessor resourceAccessor = new FileSystemResourceAccessor(
				basedir.getAbsolutePath());

		URL url = getClass().getResource("/cassandra/global/core-changelog.xml");
		File changeLog = new File(url.toURI());
		
		Liquibase liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		DatabaseChangeLog databaseChangeLog = null;
		LabelExpression labelExpression = null;
		liquibase.checkLiquibaseTables(false, databaseChangeLog, new Contexts(), labelExpression);
		CassandraDatabase cassandraDatabase = (CassandraDatabase)database;
		cassandraDatabase.clearChecksums();
	}
	
	@Ignore
	@Test
	public void updateTag() throws Exception {
		LockServiceFactory.getInstance().register(new LockServiceCassandra());
		Database database = LiquibaseExtensionUtil.createCassandraDatabase("localhost", "9160", "global_store_orders");
		assertNotNull(database.getConnection());

		File basedir = new File(System.getProperty("user.dir"));

		FileSystemResourceAccessor resourceAccessor = new FileSystemResourceAccessor(
				basedir.getAbsolutePath());

		URL url = getClass().getResource("/cassandra/global/core-changelog.xml");
		File changeLog = new File(url.toURI());
		
		Liquibase liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		liquibase.tag("tagged");
	}

}
