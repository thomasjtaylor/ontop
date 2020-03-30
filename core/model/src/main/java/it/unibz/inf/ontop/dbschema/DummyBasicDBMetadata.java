package it.unibz.inf.ontop.dbschema;

import com.google.inject.Inject;
import it.unibz.inf.ontop.dbschema.impl.BasicDBParametersImpl;

import java.util.Collection;


/**
 * A dummy DBMetadata
 */
public class DummyBasicDBMetadata implements DBMetadata {

    private final DBParameters dbParameters;

    @Inject
    private DummyBasicDBMetadata() {
        this.dbParameters = new BasicDBParametersImpl(null, null, null, null, new QuotedIDFactoryStandardSQL("\""), null);
    }

    @Override
    public DatabaseRelationDefinition getDatabaseRelation(RelationID id) { return null; }

    @Override
    public Collection<DatabaseRelationDefinition> getDatabaseRelations() { return null; }

    @Override
    public DBParameters getDBParameters() {
        return dbParameters;
    }

    public DatabaseRelationDefinition createDatabaseRelation(RelationDefinition.AttributeListBuilder builder) {
        return new DatabaseRelationDefinition(builder);
    }
}
