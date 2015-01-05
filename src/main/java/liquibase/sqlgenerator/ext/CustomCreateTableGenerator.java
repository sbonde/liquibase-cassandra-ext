package liquibase.sqlgenerator.ext;

import java.util.Map;

import liquibase.database.Database;
import liquibase.database.core.CassandraDatabase;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BigIntType;
import liquibase.datatype.core.BooleanType;
import liquibase.datatype.core.CharType;
import liquibase.datatype.core.DateTimeType;
import liquibase.datatype.core.DateType;
import liquibase.datatype.core.DecimalType;
import liquibase.datatype.core.DoubleType;
import liquibase.datatype.core.FloatType;
import liquibase.datatype.core.IntType;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.statement.PrimaryKeyConstraint;
import liquibase.statement.core.CreateTableStatement;

/**
 * @author Sanjay Bonde
 */
public class CustomCreateTableGenerator extends CreateTableGenerator  {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
	public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	if(!(database instanceof CassandraDatabase)) {
    		return super.generateSql(statement, database, sqlGeneratorChain);
    	}
        StringBuilder sql = new StringBuilder("CREATE TABLE ")
            .append(statement.getSchemaName())
            .append(".")
            .append(statement.getTableName())
            .append(" (");

        Map<String, LiquibaseDataType> dataTypes = statement.getColumnTypes();
        PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
       
        /*if(statement.getTableName().equalsIgnoreCase("DATABASECHANGELOG") && pkConstraint == null) {
        	pkConstraint = new PrimaryKeyConstraint("PK_DBCHNGLOG_ID");
        	pkConstraint.addColumns("ID");
        }*/
        if(statement.getTableName().equalsIgnoreCase("DATABASECHANGELOG") && pkConstraint == null) {
        	pkConstraint = new PrimaryKeyConstraint("PK_DBCHNGLOG");
        	pkConstraint.addColumns("ID");
        	pkConstraint.addColumns("DATEEXECUTED");
        	pkConstraint.addColumns("ORDEREXECUTED");
        }
        for (String column : statement.getColumns()) {
            String type = getDataType(dataTypes.get(column));
            sql.append(column).append(" ").append(type).append(", ");
        }

        sql.append("PRIMARY KEY (");
        for (String col : pkConstraint.getColumns()) {
            sql.append(col).append(", ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append("))");

        return new Sql[] {new UnparsedSql(sql.toString())};
    }

    private String getDataType(LiquibaseDataType dataType) {
        if (dataType instanceof BooleanType) {
            return "boolean";
        } else if (dataType instanceof IntType) {
            return "int";
        } else if (dataType instanceof BigIntType) {
            return "bigint";
        } else if (dataType instanceof FloatType) {
            return "float";
        } else if (dataType instanceof DoubleType) {
            return "double";
        } else if (dataType instanceof CharType) {
            return "varchar";
        } else if (dataType instanceof DecimalType) {
            return "decimal";
        } else if (dataType instanceof DateType || dataType instanceof DateTimeType) {
            return "timestamp";
        } else {
            throw new RuntimeException(dataType + " is not a supported type.");
        }
    }

}
