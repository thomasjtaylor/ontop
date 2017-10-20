package it.unibz.inf.ontop.model.type;

/**
 * TODO: explain
 *
 * Immutable
 */
public interface TermType {

    /**
     * Closest COL_TYPE (must be an ancestor)
     */
    @Deprecated
    COL_TYPE getColType();

    /**
     * Returns true if the TermType INSTANCE cannot be attached to a constant
     */
    boolean isAbstract();

    boolean isA(TermType otherTermType);

    TermType getCommonDenominator(TermType otherTermType);

    TermTypeAncestry getAncestry();
}
