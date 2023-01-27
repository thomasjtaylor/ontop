import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test suite for RDB2RDF tests. Updated for Microsoft SQL Server database.
 * <p>Hard-coded to connect to Ontop's ontop-mysql database on port 3694. 
 * <p><code>docker run --name ontop_mssql_running -p 1533:1433 -e "SA_PASSWORD=Mssql1.0" -e ACCEPT_EULA=Y -d ontop/ontop-mssql</code>
 * <p>Consider using mssql for better performance.
 * <p><code>docker run --rm --name ontop-sqlserver -p 1533:1433 -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=Mssql1.0" mcr.microsoft.com/mssql/server:2022-latest</code>
 * 
 * @author Thomas J. Taylor (mail@thomasjtaylor.com)
 */
@RunWith(Parameterized.class)
public class RDB2RDFTestMSSQL extends RDB2RDFTestBase {
	/**
	 * Only run the following tests. Skip others.
	 */
	private static final Set<String> ONLY = Set.of(
//			"dg0017" // failing due to unicode/utf8 problem
			);
	/**
	 * Following tests are failing due to various different reasons and bugs and are excluded manually.
	 */
	protected static final Set<String> IGNORE = Set.of(
			"tc0002f", // expected:empty, actual:data
			// Should reject an undefined SQL version
			"tc0003a",
			// Limitation of bnode isomorphism detection + xsd:double encoding (engineering notation was expected)
			"dg0005",
			// Limitation of bnode isomorphism detection, double encoding (expected:30.0, actual:30)
			"dg0005-modified",
			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
			"tc0005a",
			// Modified for H2, not MSSQL
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
			"tc0012e", // double
			"tc0012e-modified", // double
//			"dg0016", // XXX create.sql varbinary insert fails 
//			"tc0016b", // double
//			"tc0016c", // dateTime actual includes +00:00 offset
			// Should create an IRI based on a column and the base IRI. TODO: support the base IRI in R2RML
			"tc0019a"
	);
	/**
	 * Microsoft SQL Server DB Settings
	 * <li>Driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
	 * <li>URL: "jdbc:sqlserver://localhost:1533;database=RDB2RDFTest;trustServerCertificate=true"
	 */
	private static final DbSettings dbSettings = new DbSettings(
			"com.microsoft.sqlserver.jdbc.SQLServerDriver",
			"jdbc:sqlserver://localhost:1533;database=RDB2RDFTest;trustServerCertificate=true;useUnicode=true;characterEncoding=UTF-8", 
			"SA", 
			"Mssql1.0");
	
	/**
	 * Returns the list of parameters created automatically from RDB2RDF manifest files
	 */
	@Parameterized.Parameters(name="{0}")
	public static Collection<Object[]> parameters() throws Exception {
		return RDB2RDFTestBase.parameters(ONLY, IGNORE);
	}
	
	public RDB2RDFTestMSSQL(String name, String title, String sqlFile, String mappingFile, String outputFile) throws FileNotFoundException {
		super(name, title, sqlFile, mappingFile, outputFile, dbSettings);
	}

	@BeforeClass
	public static void beforeClass() {
		dropDB();
		createDB();
	}
	
	/** connect to the mssql database to create the Rdb2RdfTest schema */
	public static void createDB() {
		String database = "RDB2RDFTest";
		try (Connection c = DriverManager.getConnection(dbSettings.url.replace(";database="+database, ""), dbSettings.user, dbSettings.password);
				Statement s = c.createStatement()) {
			s.execute("CREATE DATABASE \"" + database + "\"");
		} catch (SQLException sqle) {
			System.out.println(sqle);
		}
	}
	
	/** connect to the mssql database to drop the Rdb2RdfTest schema */
	protected static void dropDB() {
		String database = "RDB2RDFTest";
		try (Connection c = DriverManager.getConnection(dbSettings.url.replace(";database="+database, ""), dbSettings.user,	dbSettings.password); 
				Statement s = c.createStatement();) {

			s.execute("ALTER DATABASE \"" + database + "\" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
					+ "DROP DATABASE \"" + database + "\"");
		} catch (SQLException sqle) {
			System.out.println(sqle);
		}
	}
	
	@Override
	protected void clearDB() {
		dropDB();
		createDB();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		RDB2RDFTestBase.afterClass();
		dropDB();
	}

}

