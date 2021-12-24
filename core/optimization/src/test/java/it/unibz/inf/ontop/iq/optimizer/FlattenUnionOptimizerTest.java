package it.unibz.inf.ontop.iq.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.iq.IntermediateQueryBuilder;
import it.unibz.inf.ontop.iq.node.ConstructionNode;
import it.unibz.inf.ontop.iq.node.ExtensionalDataNode;
import it.unibz.inf.ontop.iq.node.InnerJoinNode;
import it.unibz.inf.ontop.iq.node.UnionNode;
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom;
import it.unibz.inf.ontop.model.atom.AtomPredicate;
import it.unibz.inf.ontop.model.term.Variable;
import org.junit.Test;

import static it.unibz.inf.ontop.NoDependencyTestDBMetadata.*;
import static it.unibz.inf.ontop.OptimizationTestingTools.*;
import static junit.framework.TestCase.assertEquals;

public class FlattenUnionOptimizerTest {

    
    private final static AtomPredicate ANS1_PREDICATE1 = ATOM_FACTORY.getRDFAnswerPredicate(1);
    private final static Variable X = TERM_FACTORY.getVariable("X");
    private final static Variable Y = TERM_FACTORY.getVariable("Y");
    private final static Variable Z = TERM_FACTORY.getVariable("Z");

    @Test
    public void flattenUnionTest1() {

        IntermediateQueryBuilder queryBuilder1 = createQueryBuilder();
        DistinctVariableOnlyDataAtom projectionAtom1 = ATOM_FACTORY.getDistinctVariableOnlyDataAtom(ANS1_PREDICATE1, X);
        ConstructionNode constructionNode1 = IQ_FACTORY.createConstructionNode(projectionAtom1.getVariables());

        UnionNode unionNode1 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X));
        UnionNode unionNode2 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y));
        UnionNode unionNode3 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y, Z));

        ExtensionalDataNode dataNode1 = IQ_FACTORY.createExtensionalDataNode(TABLE1_AR2, ImmutableMap.of(0, X));
        ExtensionalDataNode dataNode2 = createExtensionalDataNode(TABLE2_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode3 = createExtensionalDataNode(TABLE4_AR3, ImmutableList.of(X, Y, Z));
        ExtensionalDataNode dataNode4 = createExtensionalDataNode(TABLE5_AR3, ImmutableList.of(X, Y, Z));

        queryBuilder1.init(projectionAtom1, constructionNode1);
        queryBuilder1.addChild(constructionNode1, unionNode1);
        queryBuilder1.addChild(unionNode1, dataNode1);
        ConstructionNode subConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder1.addChild(unionNode1, subConstructionNode1);
        queryBuilder1.addChild(subConstructionNode1, unionNode2);
        queryBuilder1.addChild(unionNode2, dataNode2);
        ConstructionNode subConstructionNode2 = IQ_FACTORY.createConstructionNode(unionNode2.getVariables());
        queryBuilder1.addChild(unionNode2, subConstructionNode2);
        queryBuilder1.addChild(subConstructionNode2, unionNode3);
        queryBuilder1.addChild(unionNode3, dataNode3);
        queryBuilder1.addChild(unionNode3, dataNode4);


        IQ query1 = queryBuilder1.buildIQ();
        System.out.println("\nBefore optimization: \n" + query1);
        IQ optimizedQuery = query1.normalizeForOptimization();
        System.out.println("\nAfter optimization: \n" + optimizedQuery);

        IntermediateQueryBuilder queryBuilder2 = createQueryBuilder();

        queryBuilder2.init(projectionAtom1, unionNode1);
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE1_AR2, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE2_AR2, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE4_AR3, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE5_AR3, ImmutableMap.of(0, X)));

        IQ query2 = queryBuilder2.buildIQ();
        System.out.println("\nExpected: \n" + query2);
        assertEquals(query2, optimizedQuery);
    }

    @Test
    public void flattenUnionTest2() {

        IntermediateQueryBuilder queryBuilder1 = createQueryBuilder();
        DistinctVariableOnlyDataAtom projectionAtom1 = ATOM_FACTORY.getDistinctVariableOnlyDataAtom(ANS1_PREDICATE1, X);

        UnionNode unionNode1 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X));
        UnionNode unionNode2 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y));
        UnionNode unionNode3 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y, Z));

        ExtensionalDataNode dataNode1 = IQ_FACTORY.createExtensionalDataNode(
                TABLE1_AR2, ImmutableMap.of(0, X));
        ExtensionalDataNode dataNode2 = createExtensionalDataNode(TABLE2_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode3 = createExtensionalDataNode(TABLE3_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode4 = createExtensionalDataNode(TABLE4_AR3, ImmutableList.of(X, Y, Z));
        ExtensionalDataNode dataNode5 = createExtensionalDataNode(TABLE5_AR3, ImmutableList.of(X, Y, Z));

        queryBuilder1.init(projectionAtom1, unionNode1);
        queryBuilder1.addChild(unionNode1, dataNode1);
        ConstructionNode subConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder1.addChild(unionNode1, subConstructionNode1);
        queryBuilder1.addChild(subConstructionNode1, unionNode2);
        ConstructionNode subConstructionNode2 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder1.addChild(unionNode1, subConstructionNode2);
        queryBuilder1.addChild(subConstructionNode2, unionNode3);
        queryBuilder1.addChild(unionNode2, dataNode2);
        queryBuilder1.addChild(unionNode2, dataNode3);
        queryBuilder1.addChild(unionNode3, dataNode4);
        queryBuilder1.addChild(unionNode3, dataNode5);


        IQ query1 = queryBuilder1.buildIQ();
        System.out.println("\nBefore optimization: \n" + query1);
        IQ optimizedQuery = query1.normalizeForOptimization();
        System.out.println("\nAfter optimization: \n" + optimizedQuery);

        IntermediateQueryBuilder queryBuilder2 = createQueryBuilder();

        queryBuilder2.init(projectionAtom1, unionNode1);
        queryBuilder2.addChild(unionNode1, dataNode1);
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE2_AR2, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE3_AR2, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE4_AR3, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE5_AR3, ImmutableMap.of(0, X)));

        IQ query2 = queryBuilder2.buildIQ();
        System.out.println("\nExpected: \n" + query2);
        assertEquals(query2, optimizedQuery);
    }


    @Test
    public void flattenUnionTest3() {

        IntermediateQueryBuilder queryBuilder1 = createQueryBuilder();
        DistinctVariableOnlyDataAtom projectionAtom1 = ATOM_FACTORY.getDistinctVariableOnlyDataAtom(ANS1_PREDICATE1, X);

        UnionNode unionNode1 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X));
        UnionNode unionNode2 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y));
        InnerJoinNode innerJoinNode = IQ_FACTORY.createInnerJoinNode();

        ExtensionalDataNode dataNode1 = IQ_FACTORY.createExtensionalDataNode(
                TABLE1_AR2, ImmutableMap.of(0, X));
        ExtensionalDataNode dataNode2 = createExtensionalDataNode(TABLE2_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode3 = createExtensionalDataNode(TABLE3_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode4 = IQ_FACTORY.createExtensionalDataNode(
                TABLE4_AR3, ImmutableMap.of(0, X, 1, Y));

        queryBuilder1.init(projectionAtom1, unionNode1);
        queryBuilder1.addChild(unionNode1, dataNode1);
        ConstructionNode subConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder1.addChild(unionNode1, subConstructionNode1);
        queryBuilder1.addChild(subConstructionNode1, innerJoinNode);
        queryBuilder1.addChild(innerJoinNode, dataNode4);
        queryBuilder1.addChild(innerJoinNode, unionNode2);
        queryBuilder1.addChild(unionNode2, dataNode2);
        queryBuilder1.addChild(unionNode2, dataNode3);


        IQ query1 = queryBuilder1.buildIQ();
        IQ query2 = query1;
        System.out.println("\nBefore optimization: \n" + query1);
        IQ optimizedQuery = query1.normalizeForOptimization();
        System.out.println("\nAfter optimization: \n" + optimizedQuery);
        assertEquals(query2, optimizedQuery);
    }

    @Test
    public void flattenUnionTest4() {

        IntermediateQueryBuilder queryBuilder1 = createQueryBuilder();
        DistinctVariableOnlyDataAtom projectionAtom1 = ATOM_FACTORY.getDistinctVariableOnlyDataAtom(ANS1_PREDICATE1, X);
        ConstructionNode constructionNode1 = IQ_FACTORY.createConstructionNode(projectionAtom1.getVariables());

        UnionNode unionNode1 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X));
        UnionNode unionNode2 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y));
        UnionNode unionNode3 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y, Z));
        InnerJoinNode innerJoinNode = IQ_FACTORY.createInnerJoinNode();

        ExtensionalDataNode dataNode1 = IQ_FACTORY.createExtensionalDataNode(TABLE1_AR2, ImmutableMap.of(0, X));
        ExtensionalDataNode dataNode2 = createExtensionalDataNode(TABLE2_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode3 = createExtensionalDataNode(TABLE3_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode4 = createExtensionalDataNode(TABLE4_AR3, ImmutableList.of(X, Y, Z));
        ExtensionalDataNode dataNode5 = createExtensionalDataNode(TABLE5_AR3, ImmutableList.of(X, Y, Z));

        queryBuilder1.init(projectionAtom1, constructionNode1);
        queryBuilder1.addChild(constructionNode1, unionNode1);
        queryBuilder1.addChild(unionNode1, dataNode1);
        ConstructionNode subConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder1.addChild(unionNode1, subConstructionNode1);
        queryBuilder1.addChild(subConstructionNode1, innerJoinNode);
        queryBuilder1.addChild(innerJoinNode, dataNode2);
        queryBuilder1.addChild(innerJoinNode, unionNode2);
        ConstructionNode subConstructionNode2 = IQ_FACTORY.createConstructionNode(unionNode2.getVariables());
        queryBuilder1.addChild(unionNode2, subConstructionNode2);
        queryBuilder1.addChild(subConstructionNode2, unionNode3);
        queryBuilder1.addChild(unionNode2, dataNode3);
        queryBuilder1.addChild(unionNode3, dataNode4);
        queryBuilder1.addChild(unionNode3, dataNode5);

        IQ query1 = queryBuilder1.buildIQ();
        System.out.println("\nBefore optimization: \n" + query1);
        IQ optimizedQuery = query1.normalizeForOptimization();
        System.out.println("\nAfter optimization: \n" + optimizedQuery);

        IntermediateQueryBuilder queryBuilder2 = createQueryBuilder();

        queryBuilder2.init(projectionAtom1, unionNode1);
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE1_AR2, ImmutableMap.of(0, X)));
        ConstructionNode newSubConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder2.addChild(unionNode1, newSubConstructionNode1);
        queryBuilder2.addChild(newSubConstructionNode1, innerJoinNode);
        queryBuilder2.addChild(innerJoinNode, dataNode2);
        queryBuilder2.addChild(innerJoinNode, unionNode2);
        queryBuilder2.addChild(unionNode2, IQ_FACTORY.createExtensionalDataNode(
                TABLE4_AR3, ImmutableMap.of(0, X, 1, Y)));
        queryBuilder2.addChild(unionNode2, IQ_FACTORY.createExtensionalDataNode(
                TABLE5_AR3, ImmutableMap.of(0, X, 1, Y)));
        queryBuilder2.addChild(unionNode2, IQ_FACTORY.createExtensionalDataNode(
                TABLE3_AR2, ImmutableMap.of(0, X, 1, Y)));

        IQ query2 = queryBuilder2.buildIQ();
        System.out.println("\nExpected: \n" + query2);
        assertEquals(query2, optimizedQuery);
    }

    @Test
    public void flattenUnionTest5() {

        IntermediateQueryBuilder queryBuilder1 = createQueryBuilder();
        DistinctVariableOnlyDataAtom projectionAtom1 = ATOM_FACTORY.getDistinctVariableOnlyDataAtom(ANS1_PREDICATE1, X);

        UnionNode unionNode1 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X));
        UnionNode unionNode2 = IQ_FACTORY.createUnionNode(ImmutableSet.of(X, Y));
        InnerJoinNode innerJoinNode = IQ_FACTORY.createInnerJoinNode();

        ExtensionalDataNode dataNode1 = IQ_FACTORY.createExtensionalDataNode(
                TABLE1_AR2, ImmutableMap.of(0, X));
        ExtensionalDataNode dataNode2 = createExtensionalDataNode(TABLE2_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode3 = createExtensionalDataNode(TABLE3_AR2, ImmutableList.of(X, Y));
        ExtensionalDataNode dataNode4 = createExtensionalDataNode(TABLE4_AR3, ImmutableList.of(X, Y, Z));
        ExtensionalDataNode dataNode5 = createExtensionalDataNode(TABLE5_AR3, ImmutableList.of(X, Y, Z));

        queryBuilder1.init(projectionAtom1, unionNode1);
        queryBuilder1.addChild(unionNode1, dataNode1);
        ConstructionNode subConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder1.addChild(unionNode1, subConstructionNode1);
        queryBuilder1.addChild(subConstructionNode1, unionNode2);
        ConstructionNode subConstructionNode2 = IQ_FACTORY.createConstructionNode(unionNode2.getVariables());
        queryBuilder1.addChild(unionNode2, subConstructionNode2);
        queryBuilder1.addChild(subConstructionNode2, innerJoinNode);
        queryBuilder1.addChild(unionNode2, dataNode2);
        queryBuilder1.addChild(innerJoinNode, dataNode3);
        queryBuilder1.addChild(innerJoinNode, dataNode4);

        IQ query1 = queryBuilder1.buildIQ();
        System.out.println("\nBefore optimization: \n" + query1);
        IQ optimizedQuery = query1.normalizeForOptimization();
        System.out.println("\nAfter optimization: \n" + optimizedQuery);

        IntermediateQueryBuilder queryBuilder2 = createQueryBuilder();

        queryBuilder2.init(projectionAtom1, unionNode1);
        queryBuilder2.addChild(unionNode1, dataNode1);
        ConstructionNode newSubConstructionNode1 = IQ_FACTORY.createConstructionNode(unionNode1.getVariables());
        queryBuilder2.addChild(unionNode1, newSubConstructionNode1);
        queryBuilder2.addChild(newSubConstructionNode1, innerJoinNode);
        queryBuilder2.addChild(unionNode1, IQ_FACTORY.createExtensionalDataNode(
                TABLE2_AR2, ImmutableMap.of(0, X)));
        queryBuilder2.addChild(innerJoinNode, dataNode3);
        queryBuilder2.addChild(innerJoinNode, IQ_FACTORY.createExtensionalDataNode(
                TABLE4_AR3, ImmutableMap.of(0, X, 1, Y)));

        IQ query2 = queryBuilder2.buildIQ();
        System.out.println("\nExpected: \n" + query2);
        assertEquals(query2, optimizedQuery);
    }
}
