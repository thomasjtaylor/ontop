/*
 * #%L
 * ontop-rdb2rdf-compliance
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import it.unibz.inf.ontop.exception.MappingBootstrappingException;
import it.unibz.inf.ontop.exception.MappingException;
import it.unibz.inf.ontop.exception.OntopResultConversionException;
import it.unibz.inf.ontop.injection.*;
import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration.Builder;
import it.unibz.inf.ontop.rdf4j.repository.OntopRepository;
import it.unibz.inf.ontop.spec.mapping.bootstrap.DirectMappingBootstrapper;
import it.unibz.inf.ontop.spec.mapping.bootstrap.DirectMappingBootstrapper.BootstrappingResults;
import it.unibz.inf.ontop.spec.mapping.pp.SQLPPMapping;
import it.unibz.inf.ontop.utils.ImmutableCollectors;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.fail;

/**
 * Test suite for RDB2RDF tests. Updated to support multiple DBMS.
 *
 * @author Evren Sirin (evren@complexible.com)
 * @author Thomas J. Taylor (thomas@infotechsoft.com)
 */
@RunWith(Parameterized.class)
public abstract class RDB2RDFTestBase {

	
	private static final List<String> SUCCESS = Lists.newArrayList();
	private static final List<String> FAILURES = Lists.newArrayList();
	private static final List<String> IGNORED = Lists.newArrayList();

	private static final String BASE_IRI = "http://example.com/base/";

	private static String LAST_SQL_SCRIPT = null;

	private static final ValueFactory FACTORY = SimpleValueFactory.getInstance();

	protected final Properties PROPERTIES;


	/**
	 * Terms used in the manifest files of RDB2RDF test suite
	 */
	private static class TestVocabulary  {
		public static final String NS = "http://purl.org/NET/rdb2rdf-test#";
		
	    public static final String DC = "http://purl.org/dc/elements/1.1/";

		public static final IRI SQL_SCRIPT_FILE = FACTORY.createIRI(NS, "sqlScriptFile");

		public static final IRI DIRECT_MAPPING = FACTORY.createIRI(NS, "DirectMapping");

		public static final IRI R2RML = FACTORY.createIRI(NS, "R2RML");

		public static final IRI MAPPING_DOCUMENT = FACTORY.createIRI(NS, "mappingDocument");

		public static final IRI OUTPUT = FACTORY.createIRI(NS, "output");
		
	    public static final IRI TITLE = FACTORY.createIRI(DC, "title");
	}

	protected static class DbSettings {
		public final String dbkey;
		public final String url;
		public final String driver;
		public final String user;
		public final String password;
		public final String database;
		public final String schema;
		public DbSettings(String driver, String url, String user, String password) {
			this(null, driver, url, user, password, null, null);
		}
		public DbSettings(String dbkey, String driver, String url, String user, String password, String database, String schema) {
			this.dbkey = Strings.nullToEmpty(dbkey);
			this.driver = driver;
			this.url = url;
			this.user = user;
			this.password = password;
			this.database = Strings.emptyToNull(database);
			this.schema = Strings.emptyToNull(schema);
		}
	}
	/**
	 * Returns the list of parameters created automatically from RDB2RDF manifest files.
	 * @param includeTests if present, only include the tests matching the provided names.
	 * @param excludeTests if present, exclude the tests matching the provided names.
	 */
	public static Collection<Object[]> parameters(Set<String> includeTests, Set<String> excludeTests) throws Exception {
		final List<Object[]> params = Lists.newArrayList();

		// There are 25 directories in the test suite so we'll iterate them all
		for (int i = 0; i < 26; i++) {
			final String dir = String.format("D%03d/", i);
			final String manifest = dir + "manifest.ttl";

			// simpler handler for manifest files that takes advantage of the fact that triples in
			// manifest files are ordered in a certain way (otherwise we'll get an explicit error)
            RDFHandler manifestHandler = new AbstractRDFHandler() {
				String name;
				String title;
				String sqlFile;
				String mappingFile;
				String outputFile;

				@Override
				public void handleStatement(final Statement st) throws RDFHandlerException {
					IRI pred = st.getPredicate();
					// the first thing we'll see in the file is the SQL script file
					if (pred.equals(TestVocabulary.SQL_SCRIPT_FILE)) {
						// make sure there is a single SQL script in each manifest
						Preconditions.checkState(sqlFile == null, "Multiple SQL files defined");
						sqlFile = dir + st.getObject().stringValue();
					}
					else if (pred.equals(RDF.TYPE)) {
						Value aType = st.getObject();
						// definition for a new test is beginning
						if (aType.equals(TestVocabulary.DIRECT_MAPPING) || aType.equals(TestVocabulary.R2RML)) {
							// create parameter for the previous test case
							createTestCase();
							// reset state
							name = ((IRI) st.getSubject()).getLocalName();
							mappingFile = outputFile = null;
						}
					}
					else if (pred.equals(TestVocabulary.MAPPING_DOCUMENT)) {
						// record the mapping file
						mappingFile = dir + st.getObject().stringValue();
					}
					else if (pred.equals(TestVocabulary.OUTPUT)) {
						// record the output file
						outputFile = dir + st.getObject().stringValue();
					}
					else if (pred.equals(TestVocabulary.TITLE)) {
					    title = st.getObject().stringValue();
					}
				}

				@Override
				public void endRDF() throws RDFHandlerException {
					// generate the parameter corresponding to the last test case
					createTestCase();
				}

				private void createTestCase() {
					// only include named tests
					if (name != null) {
						// only include tests if they are present in includeTests (or includeTests is empty)
						if (includeTests==null || includeTests.isEmpty() || includeTests.contains(name)) {
							// only include tests if they are not present in excludeTests (or excludeTests is empty)
							if (excludeTests == null || excludeTests.isEmpty() || !excludeTests.contains(name)) {
								Preconditions.checkState(sqlFile != null, "SQL file not defined");
								params.add(new Object[] { name, title, sqlFile, mappingFile, outputFile });
							} else {
								IGNORED.add(name);
							}
						}
					}
				}
			};

			// parse the manifest file
			RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
			parser.setRDFHandler(manifestHandler);
			parser.parse(RDB2RDFTestBase.class.getResourceAsStream(manifest), TestVocabulary.NS);
		}

		return params;
	}

	private URL url(String path)  {
		if (!db.dbkey.isEmpty()) {
			int where = path.lastIndexOf(".");
		    String result = path.substring(0, where) + "-" + db.dbkey + path.substring(where);
		    URL url = RDB2RDFTestBase.class.getResource(result);
		    if (url!=null) {
		    	logger.info(name+ ": DB-Specific: "+result);
		    	return url;
		    }
		}		
		return path == null ? null : RDB2RDFTestBase.class.getResource(path);
	}

	private InputStream stream(String path) {
		URL url = url(path);
		if (url!=null) {
			try {
				return url.openStream();
			} catch (IOException ioe) {
				// ignore
			}
		}
		return null;
	}

	protected final String sqlFile;
	protected final String mappingFile;
	protected final String outputFile;
	protected final String name;
	protected final String title;
	protected final DbSettings db;
	static final Logger logger = LoggerFactory.getLogger(RDB2RDFTestBase.class);
	
	public RDB2RDFTestBase(String name, String title, String sqlFile, String mappingFile, String outputFile, DbSettings dbSettings) {
		this.name = name;
		this.title = title;
		this.sqlFile = sqlFile;
		this.mappingFile = mappingFile;
		this.outputFile =  outputFile;
		this.db = dbSettings;
		PROPERTIES = new Properties();
		PROPERTIES.setProperty(OntopSQLCredentialSettings.JDBC_USER, dbSettings.user);
		PROPERTIES.setProperty(OntopSQLCredentialSettings.JDBC_PASSWORD, dbSettings.password);
		PROPERTIES.setProperty(OntopSQLCoreSettings.JDBC_URL, dbSettings.url);
		PROPERTIES.setProperty(OntopSQLCoreSettings.JDBC_DRIVER, dbSettings.driver);		
		PROPERTIES.setProperty(OntopMappingSettings.BASE_IRI, BASE_IRI);
		PROPERTIES.setProperty(OntopOBDASettings.ALLOW_RETRIEVING_BLACK_BOX_VIEW_METADATA_FROM_DB, "true");
	}
	
	protected Connection getConnection() throws SQLException {
		return DriverManager.getConnection(db.url, db.user, db.password);
	};
	
	 
	
	@Before
	public void beforeTest() throws Exception {
		logger.info("Initialize "+name+ " - "+title);
		// Several tests use the same backing database so no need to recreate the database if the previous test already did so
		if (Objects.equal(LAST_SQL_SCRIPT, sqlFile)) {
			return;
		}

		// new test so clear the database first
		if (LAST_SQL_SCRIPT != null) {
			clearDB();
		}

		LAST_SQL_SCRIPT = sqlFile;
		
        try (java.sql.Connection c = getConnection();
        	java.sql.Statement s = c.createStatement()) {
            String text = Resources.toString(url(sqlFile), Charsets.UTF_8);
            logger.debug(name+" CreateDB\r\n"+text);
            s.execute(text);
            if (logger.isDebugEnabled()) {
            	logDatabase(logger.isDebugEnabled());
            }
        } catch (SQLException sqle) {
        	sqle.printStackTrace();
        	LAST_SQL_SCRIPT = null;
            fail(name+": Exception in creating db from script: "+sqle.getLocalizedMessage());
        }
	}
		
	protected void logDatabase(boolean includeData) {
		try (Connection c = getConnection()) {
			ResultSet rs = c.getMetaData().getTables(db.database, db.schema, null, new String[] { "TABLE" });
		    while (rs.next()) {
		    	String tableName = rs.getString("TABLE_NAME");
		    	StringBuilder sb = new StringBuilder("TABLE ").append(tableName).append("\r\n");
		        ResultSet rs2 = c.getMetaData().getColumns(db.database, db.schema, tableName, null);
		        while (rs2.next()) {
		        	String columnName = rs2.getString("COLUMN_NAME");
		        	String columnType = rs2.getString("TYPE_NAME");
		        	sb.append(columnName).append(" (").append(columnType).append("),");
		        }
		        rs2.close();
		        sb.setLength(sb.length()-1);
		        sb.append("\r\n");
		        if (includeData) {
		        	java.sql.Statement s = c.createStatement();
		        	rs2 = s.executeQuery("SELECT * FROM "+tableName);
			        while (rs2.next()) {
			        	for (int columnCount = rs2.getMetaData().getColumnCount(), colnum = 1; colnum <= columnCount; colnum++) {
			        		sb.append(rs2.getString(colnum)).append(",");	
			        	}
			        	sb.setLength(sb.length()-1);
				        sb.append("\r\n");
			        }
			        rs2.close();
		        }
			    logger.debug(name+": Database Information\r\n"+sb.toString());
		    }
		} catch (SQLException sqle) {
			logger.warn(name+": "+sqle);
		}
	  }

	

	protected Repository createRepository() throws Exception {
		logger.info("createRepository " + name + " " + mappingFile);

		OntopSQLOWLAPIConfiguration configuration;
		if (mappingFile != null) {
			String absoluteFilePath = Optional.ofNullable(getClass().getResource(mappingFile))
					.map(URL::getFile)
					.orElseThrow(() -> new IllegalArgumentException("The mappingFile " + mappingFile
							+ " has not been found"));
			configuration = createStandardConfigurationBuilder()
					.r2rmlMappingFile(absoluteFilePath)
					.build();
		}
		else {
			configuration = bootstrapDMConfiguration();
		}
		OntopRepository repo = OntopRepository.defaultRepository(configuration);
		repo.init();
		return repo;
	}

	Builder<? extends Builder<?>> createStandardConfigurationBuilder() {
		  return OntopSQLOWLAPIConfiguration.defaultBuilder()
				 .properties(PROPERTIES)
				  .enableDefaultDatatypeInference(true);
	}

	Builder<? extends Builder<?>> createInMemoryBuilder() {
		return createStandardConfigurationBuilder()
				.jdbcUrl(db.url)
				.jdbcDriver(db.driver)
				.jdbcUser(db.user)
				.jdbcPassword(db.password)
				.enableDefaultDatatypeInference(true)
				.enableTestMode();
	}

	/**
	 * Bootstraps the mapping and returns a new configuration
	 */
	OntopSQLOWLAPIConfiguration bootstrapDMConfiguration()
			throws OWLOntologyCreationException, MappingException, MappingBootstrappingException {
		
		OntopSQLOWLAPIConfiguration initialConfiguration = createInMemoryBuilder().build();
		DirectMappingBootstrapper bootstrapper = DirectMappingBootstrapper.defaultBootstrapper();
		BootstrappingResults results = bootstrapper.bootstrap(initialConfiguration, BASE_IRI);

		SQLPPMapping bootstrappedMapping = results.getPPMapping();

		return createInMemoryBuilder()
				.ppMapping(bootstrappedMapping)
				.build();
	}

	/**
	 * Subclasses should override this method to clear the database between tests.
	 */
	protected void clearDB() {		
	}

	@AfterClass
	public static void afterClass() throws Exception {
		System.out.println("RDB2RDF Summary");
		System.out.println("SUCCESS " + SUCCESS.size() + " " + SUCCESS);
		System.out.println("FAILED " + FAILURES.size() + " " + FAILURES);
		System.out.println("IGNORED " + IGNORED.size() + " " + IGNORED);
		SUCCESS.clear();
		FAILURES.clear();
		IGNORED.clear();
	}

	@Test
	public void runTest() throws Exception {
//		assumeTrue(!IGNORE.contains(name));
		logger.info("runTest " + name + " - " + title);
		try {
 			runTestWithoutIgnores();
 			SUCCESS.add(name);
		}
		catch (Throwable e) {
			FAILURES.add(name);
			throw e;
		}
	}

	private void runTestWithoutIgnores() throws Exception {
		boolean outputExpected = (outputFile != null);

		Repository dataRep;

		try {
			dataRep = createRepository();
		}
		catch (Exception e) {
			// if the test is for invalid mappings then there won't be any expected output and exception here is fine
			// otherwise it is a test failure
			if (outputExpected) {
				e.printStackTrace();
				fail(name+": Error creating repository: " + e.getMessage());
			}
			return;
		}


		try (RepositoryConnection con = dataRep.getConnection()) {
			String tripleQuery = "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o}";
			logger.debug("tripleQuery " + name + " - " + tripleQuery);
			GraphQuery gquery = con.prepareGraphQuery(QueryLanguage.SPARQL, tripleQuery);
			Set<Statement> triples = QueryResults.asSet(gquery.evaluate());

			TupleQuery namedGraphQuery = con.prepareTupleQuery(QueryLanguage.SPARQL,
					"SELECT DISTINCT ?g WHERE { GRAPH ?g {?s ?p ?o } }");
			ImmutableSet<Resource> namedGraphs = QueryResults.asSet(namedGraphQuery.evaluate()).stream()
					.map(bs -> bs.getBinding("g"))
					.map(Binding::getValue)
					.map(v -> (Resource) v)
					.collect(ImmutableCollectors.toSet());

			String quadQuery = "CONSTRUCT {?s ?p ?o} WHERE { GRAPH ?g {?s ?p ?o} }";
			logger.debug("quadQuery " + name + " - " + quadQuery);
			Set<Statement> actual = new HashSet<>(triples);
			for (Resource namedGraph : namedGraphs) {
				GraphQuery query = con.prepareGraphQuery(quadQuery);
				query.setBinding("g", namedGraph);
				QueryResults.asSet(query.evaluate()).stream()
						.map(s -> FACTORY.createStatement(s.getSubject(), s.getPredicate(), s.getObject(), namedGraph))
						.forEach(actual::add);
			}

			Set<Statement> expected = ImmutableSet.of();
			if (outputExpected) {
				expected = Rio.parse(stream(outputFile), BASE_IRI, Rio.getParserFormatForFileName(outputFile).get());
			}

			if (!Models.isomorphic(expected, actual)) {
				String msg = failureMessage(expected, actual);
				System.out.println(msg);

				fail(name+": "+msg);
			}
		}
		catch (QueryEvaluationException e) {
			if (e.getCause() != null && e.getCause() instanceof OntopResultConversionException) {
				if (outputExpected) {
					e.printStackTrace();
					fail(name+": Unexpected result conversion exception: " + e.getMessage());
				}
			}
			else {
				e.printStackTrace();
				fail(name+": Unexpected exception: " + e.getMessage());
			}
		}
		finally {
			dataRep.shutDown();
		}
	}

	/**
	 * Pretty print expected and actual results
	 */
	private String failureMessage(Set<Statement> expected, Set<Statement> actual) throws RDFHandlerException {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.println("Failed Test: " + name);
		pw.println("========== Expected =============");
		Rio.write(expected, pw, RDFFormat.NQUADS);
		pw.println("========== Actual ===============");
		Rio.write(actual, pw, RDFFormat.NQUADS);
		pw.flush();
		return sw.toString();
	}
}

