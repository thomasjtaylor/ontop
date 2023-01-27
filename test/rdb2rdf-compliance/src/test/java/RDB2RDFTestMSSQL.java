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
 * 
 * <h2>Failing Tests (ontop:5.0.1-SNAPSHOT, 2023-01-27)</h2>
 * <li>tc0009c: Error creating repository: it.unibz.inf.ontop.exception.MappingIOException: it.unibz.inf.ontop.exception.MetadataExtractionException: Cannot extract metadata for a black-box view.
 * <li>dg0012-modified: Failed Test: expected != actual. row unique ids are not considered (results should repeat but do not)
 * <li>tc0014b: Error creating repository: it.unibz.inf.ontop.exception.MetaMappingExpansionException: com.microsoft.sqlserver.jdbc.SQLServerException: Conversion failed when converting the varchar value 'http://example.com/emp/' to data type int.
 * <li>tc0014c: Unexpected exception: it.unibz.inf.ontop.exception.OntopConnectionException: com.microsoft.sqlserver.jdbc.SQLServerException: Conversion failed when converting the varchar value 'http://example.com/emp/job/CLERK' to data type int.
 * <li>dg0016: Failed Test: expected != actual. varbinary data appears to be converted to UTF-8 string ("89504E4..." -> "傉䝎਍ਚ...")
 * <br>Note: tc0016e PASSES (VARBINARY/hexBinary only results), why does dg0016 FAIL w/ wrong varbinary result?
 * <li>dg0017: Failed Test: expected != actual. UTF-8 characters for generated URIs are replaced with '?'<br>
 * <pre>{@code
 * expected: <http://example.com/base/植物/名=しそ;使用部=葉> <http://example.com/base/植物#使用部> "葉" .
 *   actual: <http://example.com/base/植物/名=しそ;使用部=葉> <http://example.com/base/??#???> "葉" .
 * }</pre>
 * @author Thomas J. Taylor (mail@thomasjtaylor.com)
 */
@RunWith(Parameterized.class)
public class RDB2RDFTestMSSQL extends RDB2RDFTestBase {
	/**
	 * Only run the following tests. Skip others.
	 */
	private static final Set<String> ONLY = Set.of(
			);
	/**
	 * Following tests are failing due to various different reasons and bugs and are excluded manually.
	 */
	protected static final Set<String> IGNORE = Set.of(
			// expected:empty, actual:data
			"tc0002f", 
			// Should reject an undefined SQL version
			"tc0003a",
			// Limitation of bnode isomorphism detection + xsd:double encoding (engineering notation was expected)
			"dg0005",
			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
			"tc0005a",
			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
			"tc0005b",
			// Direct mapping and bnodes: row unique ids are not considered, leadinq to incomplete results
			// (e.g. Bob-London should appear twice). TODO: fix it
			"dg0012",
			// Modified (different XSD.DOUBLE lexical form)
			"tc0012a",
			// Modified (different XSD.DOUBLE lexical form)
			"tc0012e", // double
			// double (may also be problem with PNG hexBinary (see javadocs), tc0016c
			"dg0016", 
			"tc0016b", // double
			// Should create an IRI based on a column and the base IRI. TODO: support the base IRI in R2RML
			"tc0019a"
	);
	/**
	 * Microsoft SQL Server DB Settings
	 * <li>Driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
	 * <li>URL: "jdbc:sqlserver://localhost:1533;database=RDB2RDFTest;trustServerCertificate=true"
	 */
	private static final DbSettings dbSettings = new DbSettings(
			"mssql",
			"com.microsoft.sqlserver.jdbc.SQLServerDriver",
			"jdbc:sqlserver://localhost:1533;database=RDB2RDFTest;trustServerCertificate=true",
			"SA", 
			"Mssql1.0",
			"RDB2RDFTest",
			"dbo");
	
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
		try (Connection c = DriverManager.getConnection(dbSettings.url.replace(";database="+dbSettings.database, ""), dbSettings.user, dbSettings.password);
				Statement s = c.createStatement()) {
			s.execute("CREATE DATABASE \"" + dbSettings.database + "\"");
		} catch (SQLException sqle) {
			System.out.println(sqle);
		}
	}
	
	/** connect to the mssql database to drop the Rdb2RdfTest schema */
	protected static void dropDB() {
		try (Connection c = DriverManager.getConnection(dbSettings.url.replace(";database="+dbSettings.database, ""), dbSettings.user,	dbSettings.password); 
				Statement s = c.createStatement()) {

			s.execute("ALTER DATABASE \"" + dbSettings.database + "\" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
					+ "DROP DATABASE \"" + dbSettings.database + "\"");
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
//		dropDB();
	}

}

