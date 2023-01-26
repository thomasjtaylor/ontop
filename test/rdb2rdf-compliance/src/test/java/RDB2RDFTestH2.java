import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test suite for RDB2RDF tests. Updated for H2 database.
 * 
 * @author Thomas J. Taylor (mail@thomasjtaylor.com)
 */
@RunWith(Parameterized.class)
public class RDB2RDFTestH2 extends RDB2RDFTestBase {
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
			// Different XSD.DOUBLE lexical form; was expecting the engineering notation. Modified version added.
			"tc0005b",
			// Modified (different XSD.DOUBLE lexical form)
			"dg0012",
			// Direct mapping and bnodes: row unique ids are not considered, leadinq to incomplete results
			// (e.g. Bob-London should appear twice). TODO: fix it
			"dg0012-modified",
			// Modified (different XSD.DOUBLE lexical form)
			"tc0012a",
			// Modified (different XSD.DOUBLE lexical form)
			"tc0012e",
			// Double + timezone was not expected to be added. Same for milliseconds.
			"dg0016",
			// Different XSD.DOUBLE lexical form. Modified version added.
			"tc0016b",
			// Timezone was not expected to be added. Same for milliseconds (not so relevant test)
			"tc0016c",
			// H2 does not store the implicit trailing spaces in CHAR(15) and does not output them.
			"dg0018",
			// H2 does not store the implicit trailing spaces in CHAR(15) and does not output them.
			"tc0018a",
			// Should create an IRI based on a column and the base IRI. TODO: support the base IRI in R2RML
			"tc0019a"
	);
	/**
	 * Only run the following tests. Skip others.
	 */
	private static final Set<String> ONLY = Set.of();//"dg0011","dg0014","dg0021","dg022","dg0023","dg0024","dg0025");

	private static final DbSettings dbSettings = new DbSettings("org.h2.Driver",
			"jdbc:h2:mem:Rdb2RdfTest", "sa", "");
	
	/**
	 * Returns the list of parameters created automatically from RDB2RDF manifest files
	 */
	@Parameterized.Parameters(name="{0}")
	public static Collection<Object[]> parameters() throws Exception {
		return RDB2RDFTestBase.parameters(ONLY, IGNORE);
	}
	
	public RDB2RDFTestH2(String name, String sqlFile, String mappingFile, String outputFile) throws FileNotFoundException {
		super(name, sqlFile, mappingFile, outputFile, dbSettings);
	}


	
	@Override
	protected void clearDB() {
        try (java.sql.Connection c = getConnection();
        		java.sql.Statement s = c.createStatement()) {
            s.execute("DROP ALL OBJECTS DELETE FILES");
        } catch (SQLException sqle) {
            System.out.println("Table not found, not dropping");
        }
	}

	@AfterClass
	public static void afterClass() throws Exception {
		RDB2RDFTestBase.afterClass();
	}

}

