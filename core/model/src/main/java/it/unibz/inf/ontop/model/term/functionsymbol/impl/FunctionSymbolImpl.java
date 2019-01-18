package it.unibz.inf.ontop.model.term.functionsymbol.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.iq.node.VariableNullability;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.FunctionSymbol;
import it.unibz.inf.ontop.model.term.impl.FunctionalTermNullabilityImpl;
import it.unibz.inf.ontop.model.term.impl.PredicateImpl;
import it.unibz.inf.ontop.model.type.TermType;
import it.unibz.inf.ontop.model.type.TermTypeInference;
import it.unibz.inf.ontop.utils.ImmutableCollectors;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.IntStream;

public abstract class FunctionSymbolImpl extends PredicateImpl implements FunctionSymbol {

    private final ImmutableList<TermType> expectedBaseTypes;

    protected FunctionSymbolImpl(@Nonnull String name,
                                 @Nonnull ImmutableList<TermType> expectedBaseTypes) {
        super(name, expectedBaseTypes.size());
        this.expectedBaseTypes = expectedBaseTypes;
    }

    /**
     * TODO: REMOVE IT (TEMPORARY)
     */
    @Override
    public FunctionalTermNullability evaluateNullability(ImmutableList<? extends NonFunctionalTerm> arguments,
                                                         VariableNullability childNullability) {
        // TODO: implement it seriously
        boolean isNullable = arguments.stream()
                .filter(a -> a instanceof Variable)
                .anyMatch(a -> childNullability.isPossiblyNullable((Variable) a));
        return new FunctionalTermNullabilityImpl(isNullable);
    }

    @Override
    public ImmutableTerm simplify(ImmutableList<? extends ImmutableTerm> terms,
                                  boolean isInConstructionNodeInOptimizationPhase, TermFactory termFactory, VariableNullability variableNullability) {

        ImmutableList<ImmutableTerm> newTerms = terms.stream()
                .map(t -> (t instanceof ImmutableFunctionalTerm)
                        ? t.simplify(isInConstructionNodeInOptimizationPhase, variableNullability)
                        : t)
                .collect(ImmutableCollectors.toList());

        return buildTermAfterEvaluation(newTerms, isInConstructionNodeInOptimizationPhase, termFactory, variableNullability);
    }

    /**
     * Default implementation, to be overridden to convert more cases
     *
     * Incoming terms are not simplified as they are presumed to be already simplified
     *  (so please simplify them before)
     *
     */
    @Override
    public EvaluationResult evaluateStrictEq(ImmutableList<? extends ImmutableTerm> terms, ImmutableTerm otherTerm,
                                             TermFactory termFactory, VariableNullability variableNullability) {
        boolean differentTypeDetected = inferType(terms)
                .flatMap(TermTypeInference::getTermType)
                .map(t1 -> otherTerm.inferType()
                        .flatMap(TermTypeInference::getTermType)
                        .map(t2 -> !t1.equals(t2))
                        .orElse(false))
                .orElse(false);

        if (differentTypeDetected)
            return EvaluationResult.declareIsFalse();

        if ((otherTerm instanceof ImmutableFunctionalTerm))
            return evaluateStrictEqWithFunctionalTerm(terms, (ImmutableFunctionalTerm) otherTerm, termFactory,
                    variableNullability);
        else if ((otherTerm instanceof Constant) && otherTerm.isNull())
            return EvaluationResult.declareIsNull();
        else if (otherTerm instanceof NonNullConstant) {
            return evaluateStrictEqWithNonNullConstant(terms, (NonNullConstant) otherTerm, termFactory, variableNullability);
        }
        return EvaluationResult.declareSameExpression();
    }

    /**
     * Default implementation, can be overridden
     */
    @Override
    public EvaluationResult evaluateIsNotNull(ImmutableList<? extends ImmutableTerm> terms, TermFactory termFactory,
                                              VariableNullability variableNullability) {
        if ((!mayReturnNullWithoutNullArguments()) && (!tolerateNulls())) {
            ImmutableSet<Variable> nullableVariables = variableNullability.getNullableVariables();
            Optional<ImmutableExpression> optionalExpression = termFactory.getConjunction(terms.stream()
                    .filter(t -> (t.isNullable(nullableVariables)))
                    .map(termFactory::getDBIsNotNull));

            return optionalExpression
                    .map(e -> e.evaluate(termFactory, variableNullability)
                            .getEvaluationResult(e, true))
                    .orElseGet(EvaluationResult::declareIsTrue);
        }
        // By default, does not optimize (to be overridden for optimizing)
        return EvaluationResult.declareSameExpression();
    }

    /**
     * By default, to be overridden by function symbols that supports tolerate NULL values
     */
    @Override
    public boolean isNullable(ImmutableSet<Integer> nullableIndexes) {
        return mayReturnNullWithoutNullArguments() || (!nullableIndexes.isEmpty());
    }

    /**
     * Default implementation, can be overridden
     *
     */
    protected EvaluationResult evaluateStrictEqWithFunctionalTerm(ImmutableList<? extends ImmutableTerm> terms,
                                                                  ImmutableFunctionalTerm otherTerm,
                                                                  TermFactory termFactory,
                                                                  VariableNullability variableNullability) {
        /*
         * In case of injectivity
         */
        if (otherTerm.getFunctionSymbol().equals(this)
                && isInjective(terms, variableNullability)) {
            if (getArity() == 0)
                return EvaluationResult.declareIsTrue();

            if (!canBeSafelyDecomposedIntoConjunction(terms, variableNullability, otherTerm.getTerms()))
                /*
                 * TODO: support this special case? Could potentially be wrapped into an IF-ELSE-NULL
                 */
                return EvaluationResult.declareSameExpression();

            ImmutableExpression newExpression = termFactory.getConjunction(
                    IntStream.range(0, getArity())
                            .boxed()
                            .map(i -> termFactory.getStrictEquality(terms.get(i), otherTerm.getTerm(i)))
                            .collect(ImmutableCollectors.toList()));

            ImmutableExpression.Evaluation newEvaluation = newExpression.evaluate(termFactory, variableNullability);
            return newEvaluation.getExpression()
                    .map(EvaluationResult::declareSimplifiedExpression)
                    .orElseGet(() -> newEvaluation.getValue()
                            .map(v -> {
                                switch (v) {
                                    case TRUE:
                                        return EvaluationResult.declareIsTrue();
                                    case FALSE:
                                        return EvaluationResult.declareIsFalse();
                                    //case NULL:
                                    default:
                                        return EvaluationResult.declareIsNull();
                                }
                            })
                            .orElseThrow(() -> new MinorOntopInternalBugException(
                                    "An evaluation either is expected to return an expression or a value")));
        }
        else
            return EvaluationResult.declareSameExpression();
    }

    /**
     * ONLY for injective function symbols
     *
     * Makes sure that the conjunction would never evaluate as FALSE instead of NULL
     * (first produced equality evaluated as false, while the second evaluates as NULL)
     *
     */
    private boolean canBeSafelyDecomposedIntoConjunction(ImmutableList<? extends ImmutableTerm> terms,
                                                         VariableNullability variableNullability,
                                                         ImmutableList<? extends ImmutableTerm> otherTerms) {
        if (mayReturnNullWithoutNullArguments())
            return false;
        if (getArity() == 1)
            return true;

        return !(variableNullability.canPossiblyBeNullSeparately(terms)
                || variableNullability.canPossiblyBeNullSeparately(otherTerms));
    }

    /**
     * Default implementation, does nothing, can be overridden
     */
    protected EvaluationResult evaluateStrictEqWithNonNullConstant(ImmutableList<? extends ImmutableTerm> terms,
                                                                   NonNullConstant otherTerm, TermFactory termFactory, VariableNullability variableNullability) {
        return EvaluationResult.declareSameExpression();
    }

    /**
     * Returns true if is not guarantee to return NULL when one argument is NULL
     */
    protected abstract boolean tolerateNulls();

    /**
     * Returns false when a functional term with this symbol:
     *   1. never produce NULLs
     *   2. May produce NULLs but it is always due to a NULL argument
     */
    protected abstract boolean mayReturnNullWithoutNullArguments();

    /**
     * When the function symbol is sometimes but not always injective, please override isInjective(...)
     */
    protected abstract boolean isAlwaysInjective();

    /**
     * To be overridden when is sometimes but not always injective.
     */
    @Override
    public boolean isInjective(ImmutableList<? extends ImmutableTerm> arguments, VariableNullability variableNullability) {
        return isAlwaysInjective();
    }

    /**
     * By default, just build a new functional term.
     *
     * To be extended for reacting to null values and so on.
     *
     */
    protected ImmutableTerm buildTermAfterEvaluation(ImmutableList<ImmutableTerm> newTerms,
                                                     boolean isInConstructionNodeInOptimizationPhase,
                                                     TermFactory termFactory, VariableNullability variableNullability) {
        return termFactory.getImmutableFunctionalTerm(this, newTerms);
    }

    protected boolean isOneArgumentNull(ImmutableList<ImmutableTerm> subTerms) {
        return subTerms.stream()
                .filter(t -> t instanceof Constant)
                .anyMatch(t -> ((Constant) t).isNull());
    }

    protected ImmutableList<TermType> getExpectedBaseTypes() {
        return expectedBaseTypes;
    }

    @Override
    public TermType getExpectedBaseType(int index) {
        return expectedBaseTypes.get(index);
    }
}
