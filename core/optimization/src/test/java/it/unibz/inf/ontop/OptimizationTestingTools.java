package it.unibz.inf.ontop;


import com.google.inject.Injector;
import it.unibz.inf.ontop.datalog.DatalogFactory;
import it.unibz.inf.ontop.datalog.impl.DatalogConversionTools;
import it.unibz.inf.ontop.datalog.impl.DatalogTools;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.evaluator.ExpressionEvaluator;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.injection.OntopOptimizationConfiguration;
import it.unibz.inf.ontop.iq.optimizer.*;
import it.unibz.inf.ontop.iq.tools.IQConverter;
import it.unibz.inf.ontop.model.atom.AtomFactory;
import it.unibz.inf.ontop.model.atom.AtomPredicate;
import it.unibz.inf.ontop.model.term.Constant;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.iq.IntermediateQueryBuilder;
import it.unibz.inf.ontop.iq.tools.ExecutorRegistry;
import it.unibz.inf.ontop.model.term.ValueConstant;
import it.unibz.inf.ontop.model.term.Variable;
import it.unibz.inf.ontop.model.term.impl.ImmutabilityTools;
import it.unibz.inf.ontop.model.type.TypeFactory;
import it.unibz.inf.ontop.model.vocabulary.XSD;
import it.unibz.inf.ontop.substitution.SubstitutionFactory;

public class OptimizationTestingTools {

    private static final ExecutorRegistry EXECUTOR_REGISTRY;
    public static final IntermediateQueryFactory IQ_FACTORY;
    public static final DBMetadata EMPTY_METADATA;
    public static final JoinLikeOptimizer JOIN_LIKE_OPTIMIZER;
    public static final BindingLiftOptimizer BINDING_LIFT_OPTIMIZER;
    public static final AtomFactory ATOM_FACTORY;
    public static final TypeFactory TYPE_FACTORY;
    public static final TermFactory TERM_FACTORY;
    public static final DatalogFactory DATALOG_FACTORY;
    public static final SubstitutionFactory SUBSTITUTION_FACTORY;
    public static final PullOutVariableOptimizer PULL_OUT_VARIABLE_OPTIMIZER;
    public static final MappingUnionNormalizer MAPPING_UNION_NORMALIZER;
    public static final DatalogConversionTools DATALOG_CONVERSION_TOOLS;
    public static final ImmutabilityTools IMMUTABILITY_TOOLS;
    public static final DatalogTools DATALOG_TOOLS;
    public static final ExpressionEvaluator DEFAULT_EXPRESSION_EVALUATOR;
    public static final IQConverter IQ_CONVERTER;
    public static final ValueConstant NULL, TRUE, FALSE;
    public static final UnionAndBindingLiftOptimizer UNION_AND_BINDING_LIFT_OPTIMIZER;
    private static final DummyBasicDBMetadata DEFAULT_DUMMY_DB_METADATA;

    public static final Variable X;
    public static final Variable Y;
    public static final Variable W;
    public static final Variable Z;
    public static final Variable A;
    public static final Variable AF0;
    public static final Variable AF1;
    public static final Variable AF1F3;
    public static final Variable AF1F4;
    public static final Variable AF2;
    public static final Variable AF3;
    public static final Variable B;
    public static final Variable BF1;
    public static final Variable BF2;
    public static final Variable BF4F5;
    public static final Variable C;
    public static final Variable D;
    public static final Variable E;
    public static final Variable F;
    public static final Variable F6;
    public static final Variable F0;
    public static final Variable F0F2;
    public static final Variable F0F3;
    public static final Variable FF4;
    public static final Variable G;
    public static final Variable H;
    public static final Variable I;
    public static final Variable IF7;
    public static final Variable L;
    public static final Variable M;
    public static final Variable N;
    public static final Constant ONE, TWO;

    public static final AtomPredicate ANS1_AR0_PREDICATE, ANS1_AR1_PREDICATE, ANS1_AR2_PREDICATE, ANS1_AR3_PREDICATE,
            ANS1_AR4_PREDICATE, ANS1_AR5_PREDICATE;

    static {

        OntopOptimizationConfiguration defaultConfiguration = OntopOptimizationConfiguration.defaultBuilder()
                .enableTestMode()
                .build();

        Injector injector = defaultConfiguration.getInjector();
        EXECUTOR_REGISTRY = defaultConfiguration.getExecutorRegistry();
        IQ_FACTORY = injector.getInstance(IntermediateQueryFactory.class);
        JOIN_LIKE_OPTIMIZER = injector.getInstance(JoinLikeOptimizer.class);
        BINDING_LIFT_OPTIMIZER = injector.getInstance(BindingLiftOptimizer.class);
        ATOM_FACTORY = injector.getInstance(AtomFactory.class);
        TYPE_FACTORY = injector.getInstance(TypeFactory.class);
        TERM_FACTORY = injector.getInstance(TermFactory.class);
        DATALOG_FACTORY = injector.getInstance(DatalogFactory.class);
        DATALOG_TOOLS = injector.getInstance(DatalogTools.class);
        SUBSTITUTION_FACTORY = injector.getInstance(SubstitutionFactory.class);
        IQ_CONVERTER = injector.getInstance(IQConverter.class);
        DEFAULT_EXPRESSION_EVALUATOR = injector.getInstance(ExpressionEvaluator.class);
        UNION_AND_BINDING_LIFT_OPTIMIZER = injector.getInstance(UnionAndBindingLiftOptimizer.class);
        MAPPING_UNION_NORMALIZER = injector.getInstance(MappingUnionNormalizer.class);

        DEFAULT_DUMMY_DB_METADATA = injector.getInstance(DummyBasicDBMetadata.class);
        EMPTY_METADATA = DEFAULT_DUMMY_DB_METADATA.clone();
        EMPTY_METADATA.freeze();
        
        PULL_OUT_VARIABLE_OPTIMIZER = injector.getInstance(PullOutVariableOptimizer.class);
        DATALOG_CONVERSION_TOOLS = injector.getInstance(DatalogConversionTools.class);
        IMMUTABILITY_TOOLS = injector.getInstance(ImmutabilityTools.class);

        NULL = TERM_FACTORY.getNullConstant();
        TRUE = TERM_FACTORY.getBooleanConstant(true);
        FALSE = TERM_FACTORY.getBooleanConstant(false);

        X = TERM_FACTORY.getVariable("x");
        Y = TERM_FACTORY.getVariable("y");
        W = TERM_FACTORY.getVariable("w");
        Z = TERM_FACTORY.getVariable("z");
        A = TERM_FACTORY.getVariable("a");
        AF0 = TERM_FACTORY.getVariable("af0");
        AF1 = TERM_FACTORY.getVariable("af1");
        AF1F3 = TERM_FACTORY.getVariable("af1f3");
        AF1F4 = TERM_FACTORY.getVariable("af1f4");
        AF2 = TERM_FACTORY.getVariable("af2");
        AF3 = TERM_FACTORY.getVariable("af3");
        B = TERM_FACTORY.getVariable("b");
        BF1 = TERM_FACTORY.getVariable("bf1");
        BF2 = TERM_FACTORY.getVariable("bf2");
        BF4F5 = TERM_FACTORY.getVariable("bf4f5");
        C = TERM_FACTORY.getVariable("c");
        D = TERM_FACTORY.getVariable("d");
        E = TERM_FACTORY.getVariable("e");
        F = TERM_FACTORY.getVariable("f");
        F6 = TERM_FACTORY.getVariable("f6");
        F0 = TERM_FACTORY.getVariable("f0");
        F0F2 = TERM_FACTORY.getVariable("f0f2");
        F0F3 = TERM_FACTORY.getVariable("f0f3");
        FF4 = TERM_FACTORY.getVariable("ff4");
        G = TERM_FACTORY.getVariable("g");
        H = TERM_FACTORY.getVariable("h");
        I = TERM_FACTORY.getVariable("i");
        IF7 = TERM_FACTORY.getVariable("if7");
        L = TERM_FACTORY.getVariable("l");
        M = TERM_FACTORY.getVariable("m");
        N = TERM_FACTORY.getVariable("n");
        ONE = TERM_FACTORY.getConstantLiteral("1", XSD.INTEGER);
        TWO = TERM_FACTORY.getConstantLiteral("2", XSD.INTEGER);

        ANS1_AR0_PREDICATE = ATOM_FACTORY.getAtomPredicate("ans1", 0);
        ANS1_AR1_PREDICATE = ATOM_FACTORY.getAtomPredicate("ans1", 1);
        ANS1_AR2_PREDICATE = ATOM_FACTORY.getAtomPredicate("ans1", 2);
        ANS1_AR3_PREDICATE = ATOM_FACTORY.getAtomPredicate("ans1", 3);
        ANS1_AR4_PREDICATE = ATOM_FACTORY.getAtomPredicate("ans1", 4);
        ANS1_AR5_PREDICATE = ATOM_FACTORY.getAtomPredicate("ans1", 5);
    }

    public static IntermediateQueryBuilder createQueryBuilder(DBMetadata metadata) {
        return IQ_FACTORY.createIQBuilder(metadata, EXECUTOR_REGISTRY);
    }

    public static BasicDBMetadata createDummyMetadata() {
        return DEFAULT_DUMMY_DB_METADATA.clone();
    }
}
