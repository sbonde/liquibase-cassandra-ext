package liquibase;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;

import org.junit.Ignore;
import org.junit.Test;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.integration.commandline.CommandLineUtils;
import liquibase.resource.FileSystemResourceAccessor;

/**
 * @author Sanjay Bonde
 */
@Ignore
public class OracleProvisionTest {

	@Test
	public void runLiquibase() throws Exception {
		Database database = createDatabase("dbaasuser");
		assertNotNull(database.getConnection());

		//File basedir = new File(System.getProperty("project.basedir"));
		File basedir = new File(System.getProperty("user.dir"));

		FileSystemResourceAccessor resourceAccessor = new FileSystemResourceAccessor(
				basedir.getAbsolutePath());

		URL url = getClass().getResource("/oracle/presys-changelog.xml");
		File changeLog = new File(url.toURI());
		
		Liquibase liquibase = new Liquibase(changeLog.getAbsolutePath(),
				resourceAccessor, database);
		String contexts = "";
		liquibase.update(contexts);
		System.out.println("Finished");
	}

	private Database createDatabase(String schema) throws Exception {
		
		String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.227.64.117)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=oradb_ops.gecwalmart.com)))";
		String username = "dbaasuser";
		String password = "pangaea";
		String driver = "oracle.jdbc.driver.OracleDriver";
		String databaseClass = "liquibase.database.core.OracleDatabase";
		String defaultCatalog = "";
		String defaultSchema = schema;
		boolean outputDefaultCatalog = false;
		boolean outputDefaultSchema = false;
		String driverPropertiesFile = null;
		String liquibaseCatalogName = null;
		String liquibaseSchemaName = null;
		String propertyProviderClass = null;
		return CommandLineUtils.createDatabaseObject(getClass().getClassLoader(), url, username, password, driver, defaultCatalog, defaultSchema, outputDefaultCatalog, outputDefaultSchema, databaseClass, driverPropertiesFile, propertyProviderClass, liquibaseCatalogName, liquibaseSchemaName);
	}
	

}
