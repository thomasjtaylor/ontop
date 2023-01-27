import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test suite for RDB2RDF tests. Updated for MySQL database.
 * <p>Hard-coded to connect to Ontop's ontop-mysql database on port 3694. 
 * <p><code>docker run --name ontop_mysql_running -p 3694:3306 -e MYSQL_ROOT_PASSWORD=mysql -d ontop/ontop-mysql</code>
 * <p>Consider using mysql for better performance.
 * <p><code>docker run --rm --name ontop-mysql -p 3694:3306 -e MYSQL_ROOT_PASSWORD=mysql mysql:5</code>
 * 
 * <h2>Test Failures (ontop:5.0.1-SNAPSHOT, 2023-01-27)</h2>
 * <li>tc0002d: Error creating repository: java.lang.UnsupportedOperationException: Not supported by MySQL
 * <li>tc0003b: Error creating repository: java.lang.UnsupportedOperationException: Not supported by MySQL
 * <li>dg0012-modified: Failed Test: expected != actual. expected repeat nodes (Bob,Smith,30; Bob,Smith,London) only occurs once in actual
 * <li>dg0014: Failed Test: expected != actual.
 * <pre>{@code
 * missing expected: <http://example.com/base/EMP/empno=7369> <http://example.com/base/EMP#ref-deptno> _:genid2d78245a7f3d6e4a7da8caf0dc446b080b4132dc . }</pre>
 * <li>tc0014a: Error creating repository: java.lang.UnsupportedOperationException: Not supported by MySQL
 * <li>tc0014b: Error creating repository: java.lang.UnsupportedOperationException: Not supported by MySQL
 * <li>tc0014c: Error creating repository: java.lang.UnsupportedOperationException: Not supported by MySQL
 * @author Thomas J. Taylor (mail@thomasjtaylor.com)
 */
@RunWith(Parameterized.class)
public class RDB2RDFTestMySQL extends RDB2RDFTestBase {
	/**
	 * Only run the following tests. Skip others.
	 */
	private static final Set<String> ONLY = Set.of(

			);
	/**
	 * Following tests are failing due to various different reasons and bugs and are excluded manually.
	 */
	protected static final Set<String> IGNORE = Set.of(
//			"tc0002d", // Ontop: MySQL UnsupportedOperationException
//			"tc0003b", // Ontop: MySQL UnsupportedOperationException
//			"tc0014a", // Ontop: MySQL UnsupportedOperationException
//			"tc0014b", // Ontop: MySQL UnsupportedOperationException
//			"tc0014c", // Ontop: MySQL UnsupportedOperationException
			"tc0002f", // expected:empty, actual:data
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
			"tc0012e", // double
			"dg0016", // double
			"tc0016b", // double
			"tc0016c", // dateTime actual includes +00:00 offset
			// Should create an IRI based on a column and the base IRI. TODO: support the base IRI in R2RML
			"tc0019a"
	);
	/**
	 * MySQL DB Settings
	 * <li>Driver: com.mysql.cj.jdbc.Driver
	 * <li>URL: "jdbc:mysql://:3694/Rdb2RdfTest?characterEncoding=utf8&sessionVariables=sql_mode='ANSI'&allowMultiQueries=true&nullDatabaseMeansCurrent=true"
	 * <li>- characterEncoding=utf8 - for unicode tests
	 * <li>- sessionVariables=sql_mode
	 * <li>--  sql_mode='ANSI' - to support ANSI-formatted create.sql scripts
	 * <li>--  sql_mode=PAD_CHAR_TO_FULL_LENGTH - 
	 * <li>- allowMultiQueries=true - allows multi-statement create.sql to execute w/o splitting by ';'
	 * <li>- nullDatabaseMeansCurrent=true - since mysql:8 default 'false' (mysql:5 was 'true') 
	 */
	private static final DbSettings dbSettings = new DbSettings(
			"mysql",
			"com.mysql.cj.jdbc.Driver",
			"jdbc:mysql://:3694/Rdb2RdfTest?characterEncoding=utf8&sessionVariables=sql_mode='ANSI,PAD_CHAR_TO_FULL_LENGTH'&allowMultiQueries=true&nullDatabaseMeansCurrent=true", 
			"root", 
			"mysql",
			"Rdb2RdfTest",
			null);
	
	/**
	 * Returns the list of parameters created automatically from RDB2RDF manifest files
	 */
	@Parameterized.Parameters(name="{0}")
	public static Collection<Object[]> parameters() throws Exception {
		return RDB2RDFTestBase.parameters(ONLY, IGNORE);
	}
	
	public RDB2RDFTestMySQL(String name, String title, String sqlFile, String mappingFile, String outputFile) throws FileNotFoundException {
		super(name, title, sqlFile, mappingFile, outputFile, dbSettings);
	}

	@BeforeClass
	public static void beforeClass() {
		dropDB();
		createDB();
	}
	
	public static void createDB() {
		String database = "Rdb2RdfTest";
		try (java.sql.Connection pgConnection = DriverManager
				.getConnection(dbSettings.url.replace(database, ""), dbSettings.user, dbSettings.password);
				java.sql.Statement s = pgConnection.createStatement()) {
			s.execute("CREATE SCHEMA IF NOT EXISTS \"" + database + "\" CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'");
		} catch (SQLException sqle) {
			System.out.println(sqle);
		}
	}
	
	/** connect to the postgres database to drop the Rdb2RdfTest schema */
	protected static void dropDB()  {
		String database = "Rdb2RdfTest";
		try (java.sql.Connection pgConnection = DriverManager
				.getConnection(dbSettings.url.replace(database, ""), dbSettings.user, dbSettings.password);
				java.sql.Statement s = pgConnection.createStatement()) {
			s.execute("DROP SCHEMA IF EXISTS \"" + database + "\"");
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

