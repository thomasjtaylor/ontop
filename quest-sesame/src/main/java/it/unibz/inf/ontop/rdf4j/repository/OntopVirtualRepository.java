package it.unibz.inf.ontop.rdf4j.repository;

/*
 * #%L
 * ontop-quest-sesame
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

import it.unibz.inf.ontop.injection.QuestConfiguration;
import it.unibz.inf.ontop.model.OBDAException;
import it.unibz.inf.ontop.owlrefplatform.core.QuestDBConnection;
import it.unibz.inf.ontop.owlrefplatform.questdb.QuestDBVirtualStore;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OntopVirtualRepository extends AbstractOntopRepository {

	private final QuestDBVirtualStore virtualStore;
	private QuestDBConnection questDBConn;
	private boolean initialized = false;
	private static final Logger logger = LoggerFactory.getLogger(AbstractOntopRepository.class);
	
	public OntopVirtualRepository(QuestConfiguration configuration) {
		super();
		this.virtualStore = new QuestDBVirtualStore(configuration);
	}

	/**
	 * Returns a new RepositoryConnection.
	 *
	 * (No repository connection sharing for the sake
	 *  of thread-safeness)
	 *
	 */
	public RepositoryConnection getConnection() throws RepositoryException {
		try {
			return new OntopRepositoryConnection(this, getQuestConnection());
		} catch (OBDAException e) {
			logger.error("Error creating repo connection: " + e.getMessage());
			throw new RepositoryException(e.getMessage());
		}
	}


	/**
	 * This method leads to the reasoner being initialized (connecting to the database,
	 * analyzing mappings, etc.). This must be called before any queries are run, i.e. before {@code getConnection}.
	 * 
	 */
	@Override
	public void initialize() throws RepositoryException{
		initialized = true;
		try {
			this.virtualStore.initialize();
		}
		catch (Exception e){
			throw new RepositoryException(e);
		}
	}
	
	/**
	 * Returns a connection which can be used to run queries over the repository
	 * Before this method can be used, initialize() must be called once.
	 */
	private QuestDBConnection getQuestConnection() throws RepositoryException {
		if(!initialized)
			throw new RepositoryException("The OntopVirtualRepository must be initialized before getConnection can be run.");
		try {
			questDBConn = this.virtualStore.getConnection();
			return questDBConn;
		} catch (OBDAException e) {
			throw new RepositoryException(e.getMessage());
		}

	}

	@Override
	public boolean isWritable() throws RepositoryException {
		return false;
	}

	@Override
	public boolean isInitialized() {
		return initialized;
	}
	
	@Override
	public void shutDown() throws RepositoryException {
		initialized = false;
		try {
			questDBConn.close();
			virtualStore.close();
		} catch (OBDAException e) {
			e.printStackTrace();
		}
	}
}
