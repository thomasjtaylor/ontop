import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * Test suite for RDB2RDF tests. Updated for Postgresql database.
 * <p>Hard-coded to connect to Ontop's ontop-pgsql database on port 7777. Consider using postgres:13 for better performance.
 * <p><code>docker run --name ontop_postgres_running -p 7777:5432 -e POSTGRES_PASSWORD=postgres2 -d ontop/ontop-pgsql</code>
 * 
 * @author Thomas J. Taylor (mail@thomasjtaylor.com)
 */
@RunWith(Parameterized.class)
public class RDB2RDFTestPostgres extends RDB2RDFTestBase {
	/**
	 * Only run the following tests. Skip others.
	 */
	private static final Set<String> ONLY = Set.of();//"dg0011","dg0014","dg0021","dg022","dg0023","dg0024","dg0025");
	/**
	 * Following tests are failing due to various different reasons and bugs and are excluded manually.
	 */
	protected static final Set<String> IGNORE = Set.of(
			// Should reject an undefined SQL version
			"tc0003a",
			// Limitation of bnode isomorphism detection + xsd:double encoding (engineering notation was expected)
			"dg0005",
			// Limitation of bnode isomorphism detection
			"dg0005-modified",
			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
			"tc0005a",
			// Modified for H2, not PSQL
			"tc0005a-modified",
			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
			"tc0005b",
			"tc0005b-modified",
			// Modified (different XSD.DOUBLE lexical form)
			"dg0012",
			// Direct mapping and bnodes: row unique ids are not considered, leadinq to incomplete results
			// (e.g. Bob-London should appear twice). TODO: fix it
			"dg0012-modified",
			// Modified (different XSD.DOUBLE lexical form)
			"tc0012a",
			"tc0012a-modified",
			// Modified (different XSD.DOUBLE lexical form)
			"tc0012e",
			"tc0012e-modified",
			// "dg0014" - ERROR: No function matches replace(integer, unknown, unknown)
			// PSQL "varbinary" does not exist; TODO change varbinary to bytea, use decode('XXX','hex') in create.sql
			"dg0016",
			"tc0016a",
			"tc0016b",
			"tc0016b-modified",
			"tc0016c",
			"tc0016d",
			"tc0016e",
			// PSQL does not store the implicit trailing spaces in CHAR(15) and does not output them.
			"dg0018",
			// PSQL does not store the implicit trailing spaces in CHAR(15) and does not output them.
			"tc0018a",
			// Should create an IRI based on a column and the base IRI. TODO: support the base IRI in R2RML
			"tc0019a"
	);
	private static final DbSettings dbSettings = new DbSettings("org.postgresql.Driver",
			"jdbc:postgresql://:7777/Rdb2RdfTest", "postgres", "postgres2");
	
	/**
	 * Returns the list of parameters created automatically from RDB2RDF manifest files
	 */
	@Parameterized.Parameters(name="{0}")
	public static Collection<Object[]> parameters() throws Exception {
		return RDB2RDFTestBase.parameters(ONLY, IGNORE);
	}
	
	public RDB2RDFTestPostgres(String name, String sqlFile, String mappingFile, String outputFile) throws FileNotFoundException {
		super(name, sqlFile, mappingFile, outputFile, dbSettings);
	}

	@BeforeClass
	public static void createDB() {
		try (java.sql.Connection pgConnection = DriverManager
				.getConnection(dbSettings.url.replace("Rdb2RdfTest", "postgres"), dbSettings.user, dbSettings.password);
				java.sql.Statement s = pgConnection.createStatement()) {
			String database = "Rdb2RdfTest";
			s.execute("CREATE DATABASE \"" + database + "\"");
		} catch (SQLException sqle) {
			System.out.println(sqle);
		}
	}
	
	/** connect to the postgres database to drop the Rdb2RdfTest schema */
	protected static void dropDB()  {
        try (java.sql.Connection pgConnection = DriverManager.getConnection(dbSettings.url.replace("Rdb2RdfTest", "postgres"), dbSettings.user, dbSettings.password);
                java.sql.Statement s = pgConnection.createStatement()) {
                String database = "Rdb2RdfTest";
                try {
                s.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) "
                        + "FROM pg_stat_activity "
                        + "WHERE datname = '"+database+"' "
                                + "AND pid <> pg_backend_pid(); "
                        + "DROP DATABASE \""+database+"\"");
                } catch (SQLException sqle) {}
                s.execute("CREATE DATABASE \""+database+"\"");
              } catch (SQLException sqle) {
                  System.out.println(sqle);
              }
	}
	
	@Override
	protected void clearDB() {
		dropDB();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		RDB2RDFTestBase.afterClass();
		dropDB();
	}

}

