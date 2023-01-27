import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Test suite for RDB2RDF tests. Updated for Oracle database.
 * <p><code>docker run --rm --name ontop_oracle_running -p 49161:1521 -e "ORACLE_PWD=oracle" -e LICENSE=accept ontop/ontop-oracle:2.0</code>

 * <p><code>docker run --rm --name ontop-oracle -p 49161:1521 -e "ORACLE_PWD=oracle" container-registry.oracle.com/database/express:21.3.0-xe</code>
 * 
 * @author Thomas J. Taylor (mail@thomasjtaylor.com)
 */
@RunWith(Parameterized.class)
public class RDB2RDFTestOracle extends RDB2RDFTestBase {
	/**
	 * Only run the following tests. Skip others.
	 */
	private static final Set<String> ONLY = Set.of();
	/**
	 * Following tests are failing due to various different reasons and bugs and are excluded manually.
	 */
	protected static final Set<String> IGNORE = Set.of(
//			// Should reject an undefined SQL version
//			"tc0003a",
//			// Limitation of bnode isomorphism detection + xsd:double encoding (engineering notation was expected)
//			"dg0005",
//			// Limitation of bnode isomorphism detection
//			"dg0005-modified",
//			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
//			"tc0005a",
//			// Modified for H2, not PSQL
//			"tc0005a-modified",
//			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
//			"tc0005b",
//			"tc0005b-modified",
//			// Modified (different XSD.DOUBLE lexical form)
//			"dg0012",
//			// Direct mapping and bnodes: row unique ids are not considered, leadinq to incomplete results
//			// (e.g. Bob-London should appear twice). TODO: fix it
//			"dg0012-modified",
//			// Modified (different XSD.DOUBLE lexical form)
//			"tc0012a",
//			"tc0012a-modified",
//			// Modified (different XSD.DOUBLE lexical form)
//			"tc0012e",
//			"tc0012e-modified",
//			// "dg0014" - ERROR: No function matches replace(integer, unknown, unknown)
//			// PSQL "varbinary" does not exist; TODO change varbinary to bytea, use decode('XXX','hex') in create.sql
//			"dg0016",
//			"tc0016a",
//			"tc0016b",
//			"tc0016b-modified",
//			"tc0016c",
//			"tc0016d",
//			"tc0016e",
//			// PSQL does not store the implicit trailing spaces in CHAR(15) and does not output them.
//			"dg0018",
//			// PSQL does not store the implicit trailing spaces in CHAR(15) and does not output them.
//			"tc0018a",
//			// Should create an IRI based on a column and the base IRI. TODO: support the base IRI in R2RML
//			"tc0019a"
	);
	private static final DbSettings dbSettings = new DbSettings(
			"oracle",
			"oracle.jdbc.OracleDriver",
			"jdbc:oracle:thin:@//:1521/XEPDB1",
			"Rdb2RdfTest", // user
			"Rdb2RdfTest", // password
			null, // database
			"Rdb2RdfTest".toUpperCase()); // schema
	
	/**
	 * Returns the list of parameters created automatically from RDB2RDF manifest files
	 */
	@Parameterized.Parameters(name="{0}")
	public static Collection<Object[]> parameters() throws Exception {
		return RDB2RDFTestBase.parameters(ONLY, IGNORE);
	}
	
	public RDB2RDFTestOracle(String name, String title, String sqlFile, String mappingFile, String outputFile) throws FileNotFoundException {
		super(name, title, sqlFile, mappingFile, outputFile, dbSettings);
	}

	@BeforeClass
	public static void createDB() {
		dropDB();
		try (Connection c = DriverManager.getConnection(dbSettings.url, "system", dbSettings.password);
				Statement s = c.createStatement()) {
			try {
			// create tablespace
            s.execute("ALTER SYSTEM SET DB_CREATE_FILE_DEST = \"/tmp\"");
            s.execute("CREATE TABLESPACE " + dbSettings.user + "_TS");
            s.execute("CREATE TEMPORARY TABLESPACE " + dbSettings.user + "_TS_Temp");
			} catch (SQLException sqle) {
				logger.info("Create Tablespace: "+sqle);
			}
			// create test user/schema
            s.execute(
                    "CREATE USER " + dbSettings.user + " IDENTIFIED BY " + dbSettings.password
                            + " DEFAULT TABLESPACE  " + dbSettings.user + "_TS"
                            + " TEMPORARY TABLESPACE " + dbSettings.user + "_TS_Temp");
            s.execute("GRANT CREATE SESSION TO " + dbSettings.user + "");
            s.execute("GRANT ALL PRIVILEGES TO " + dbSettings.user);
		} catch (SQLException sqle) {
			logger.error("Create User: "+sqle);
		}
	}
	
	/** connect to the postgres database to drop the Rdb2RdfTest schema */
	protected static void dropDB()  {
        try (Connection c = DriverManager.getConnection(dbSettings.url, "system", dbSettings.password);
                java.sql.Statement s = c.createStatement()) {
            // close existing connections
            try {
              s.execute(
                      "BEGIN\n"
                              + "   FOR ln_cur IN (SELECT sid, serial# FROM v$session WHERE username = '"
                              + dbSettings.user.toUpperCase() + "')\n" + "   LOOP\n"
                              + "      EXECUTE IMMEDIATE ('ALTER SYSTEM KILL SESSION ''' || ln_cur.sid || ',' || ln_cur.serial# || ''' IMMEDIATE');\n"
                              + "   END LOOP;\n" + "END;");
            } catch (Exception ex) {
              System.out.println("Oracle Kill Session: " + ex.getLocalizedMessage());
            }
            // drop test user/schema
            // https://stackoverflow.com/questions/68573313/how-creating-a-new-database-schema-for-an-oracle-18c-xe-installation
            // https://docs.oracle.com/cd/E11882_01/server.112/e41084/statements_7003.htm#SQLRF01403
            try {
              s.execute("DROP USER " + dbSettings.user + " CASCADE");
            } catch (Exception ex) {
              System.out.println("Oracle Drop User: " + ex.getLocalizedMessage());
            }
        } catch (SQLException sqle) {
        	logger.warn(sqle.getLocalizedMessage());
        }
	}
	
	@Override
	protected void clearDB() {
		createDB();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		RDB2RDFTestBase.afterClass();
		dropDB();
	}

}

