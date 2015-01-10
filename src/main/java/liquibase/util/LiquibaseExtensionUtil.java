package liquibase.util;

import java.net.URI;
import java.text.MessageFormat;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.integration.commandline.CommandLineUtils;

/**
 * 
 * @author sbonde
 *
 */
public class LiquibaseExtensionUtil {
	
	public LiquibaseExtensionUtil(){
		
	}
	/**
	 * Create a Cassandra Database object using connection string and schema
	 * @param connString
	 * @param schema
	 * @return
	 * @throws Exception
	 */
	public static Database createCassandraDatabase(String host, String port, String schema) throws Exception {
		String connString = "jdbc:cassandra://{0}:{1}/{2}?version=3.0.0";
		connString = MessageFormat.format(connString, host, port, schema);
		return createCassandraDatabase(connString, schema);
	}
	
	public static Database createCassandraDatabase(String connString,
			String schema) throws DatabaseException {
	    	String url = adjustConnString(connString, schema);
			String username = "";
			String password = "";
			String driver = "org.apache.cassandra.cql.jdbc.CassandraDriver";
			String databaseClass = "liquibase.database.core.CassandraDatabase";
			String defaultCatalogName = "";
			//String defaultSchema = schema;
			String defaultSchemaName = null;
			boolean outputDefaultCatalog = false;
			boolean outputDefaultSchema = false;
			String driverPropertiesFile = null;
			String propertyProviderClass = null;
			String liquibaseCatalogName = null;
			//String liquibaseSchemaName = null;
			String liquibaseSchemaName = schema;
			return CommandLineUtils.createDatabaseObject(new LiquibaseExtensionUtil().getClass().getClassLoader(), url, username, password, driver, defaultCatalogName, defaultSchemaName, outputDefaultCatalog, outputDefaultSchema, databaseClass, driverPropertiesFile, propertyProviderClass, liquibaseCatalogName, liquibaseSchemaName);	
	}
	   
	   public static String adjustConnString(String connString, String username) {
	    	if(connString == null || connString.equals("")) {
	    		return connString;
	    	}
			String cleanURI = connString.substring(5);

			URI uri = URI.create(cleanURI);
			return connString.replace(uri.getPath(), "/"+username);
		}

}
