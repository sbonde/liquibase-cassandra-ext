package liquibase.util;

import java.net.URI;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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
		//String connString = "jdbc:cassandra://{0}:{1}/{2}";
		//String connString = "jdbc:cassandra://10.242.175.58:{0}/{1}?version=4.0.3,jdbc:cassandra://10.227.66.10:{0}/{1}?version=4.0.3";
		//String connString = "jdbc:cassandra://10.242.175.58:{0}/{1},jdbc:cassandra://10.227.66.10:{0}/{1}?version=3.1.";
		connString = MessageFormat.format(connString, host, port, schema);
		//connString = MessageFormat.format(connString, port, schema);
		return createCassandraDatabase(connString, schema);
	}
	
	public static Database createCassandraDatabase(String connString,
			String schema) throws DatabaseException {
	    	String url = adjustConnString(connString, schema);
			String username = "";
			String password = "";
			String driver = "org.apache.cassandra.cql.jdbc.CassandraDriver";
			String databaseClass = "liquibase.database.core.CassandraDatabase";
			String defaultCatalog = "";
			//String defaultSchema = schema;
			String defaultSchema = null;
			boolean outputDefaultCatalog = false;
			boolean outputDefaultSchema = false;
			String driverPropertiesFile = null;
			String liquibaseCatalogName = null;
			//String liquibaseSchemaName = null;
			String liquibaseSchemaName = schema;
			return CommandLineUtils.createDatabaseObject(new LiquibaseExtensionUtil().getClass().getClassLoader(), url, username, password, driver, defaultCatalog, defaultSchema, outputDefaultCatalog, outputDefaultSchema, databaseClass, driverPropertiesFile, liquibaseCatalogName, liquibaseSchemaName);
		}
	   
	   public static String adjustConnString(String connString, String username) {
	    	if(connString == null || connString.equals("")) {
	    		return connString;
	    	}
			String cleanURI = connString.substring(5);

			URI uri = URI.create(cleanURI);
			return connString.replace(uri.getPath(), "/"+username);
		}

	   public static void main(String[] args) {
		   String tmpDateExecuted = "Thu Aug 28 17:00:00 PDT 2014";
		   DateFormat df = new SimpleDateFormat(
					"E MMM dd HH:mm:ss z yyyy");
		   Date dateExecuted = null;
			try {
				dateExecuted = df.parse(tmpDateExecuted);
			} catch (Exception e) {
				dateExecuted = null;
			}
			System.out.println("dateExecuted = "+dateExecuted);
	   }

}
