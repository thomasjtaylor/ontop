package it.unibz.inf.ontop.model.atom;


import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.Predicate;
import it.unibz.inf.ontop.model.type.TermType;
import org.apache.commons.rdf.api.IRI;

public interface AtomFactory {

    @Deprecated
    AtomPredicate getAtomPredicate(String name, int arity);

    AtomPredicate getAtomPredicate(String name, ImmutableList<TermType> expectedBaseTypes);

    AtomPredicate getAtomPredicate(Predicate datalogPredicate);

    /**
     * Beware: a DataAtom is immutable
     */
    <P extends AtomPredicate> DataAtom<P> getDataAtom(P predicate, ImmutableList<? extends VariableOrGroundTerm> terms);

    /**
     * Beware: a DataAtom is immutable
     */
    <P extends AtomPredicate> DataAtom<P> getDataAtom(P predicate, VariableOrGroundTerm... terms);

    DistinctVariableDataAtom getDistinctVariableDataAtom(AtomPredicate predicate,
                                                         ImmutableList<? extends VariableOrGroundTerm> arguments);
    DistinctVariableDataAtom getDistinctVariableDataAtom(AtomPredicate predicate, VariableOrGroundTerm ... arguments);

    DistinctVariableOnlyDataAtom getDistinctVariableOnlyDataAtom(AtomPredicate predicate,
                                                                 ImmutableList<Variable> arguments);

    DistinctVariableOnlyDataAtom getDistinctVariableOnlyDataAtom(AtomPredicate predicate,
                                                                 Variable ... arguments);

    VariableOnlyDataAtom getVariableOnlyDataAtom(AtomPredicate predicate, Variable... terms);

    VariableOnlyDataAtom getVariableOnlyDataAtom(AtomPredicate predicate, ImmutableList<Variable> terms);

    Function getMutableTripleAtom(Term subject, Term predicate, Term object);

    Function getMutableTripleAtom(Term subject, IRI propertyIRI, Term object);

    Function getMutableTripleAtom(Term subject, IRI classIRI);

    TriplePredicate getTripleAtomPredicate();

    ImmutableFunctionalTerm getTripleAtom(VariableOrGroundTerm subject, VariableOrGroundTerm property, VariableOrGroundTerm object);
    ImmutableFunctionalTerm getTripleAtom(VariableOrGroundTerm subject, IRI propertyIRI, VariableOrGroundTerm object);
    ImmutableFunctionalTerm getTripleAtom(VariableOrGroundTerm subject, IRI classIRI);
}
