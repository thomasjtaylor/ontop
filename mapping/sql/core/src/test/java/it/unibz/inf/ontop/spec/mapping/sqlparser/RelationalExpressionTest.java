package it.unibz.inf.ontop.spec.mapping.sqlparser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.dbschema.impl.OfflineMetadataProviderBuilder;
import it.unibz.inf.ontop.iq.node.ExtensionalDataNode;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.spec.mapping.sqlparser.exception.IllegalJoinException;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Before;
import org.junit.Test;

import static it.unibz.inf.ontop.utils.SQLMappingTestingTools.*;
import static org.junit.Assert.*;

/**
 * Created by Roman Kontchakov on 01/11/2016.
 *
 */

// TODO: REFACTOR

public class RelationalExpressionTest {

    private static QuotedIDFactory MDFAC;

    private DatabaseRelationDefinition P;

    private ExtensionalDataNode f1, f2;
    private ImmutableFunctionalTerm eq;
    private Variable x, y, u, v;
    private QualifiedAttributeID qaTx;
    private QualifiedAttributeID qaTy;
    private QualifiedAttributeID qaNx;
    private QualifiedAttributeID qaNy;
    private QualifiedAttributeID qaTu;
    private QualifiedAttributeID qaTv;
    private QualifiedAttributeID qaNv;
    private QuotedID attX, attY;
    private RAExpression re1, re2, re1_1, re3;
    private EqualsTo onExpression;

    @Before
    public void setupTest(){
        OfflineMetadataProviderBuilder builder = createMetadataProviderBuilder();
        MDFAC = builder.getQuotedIDFactory();

        x = TERM_FACTORY.getVariable("x");
        y = TERM_FACTORY.getVariable("y");

        DBTermType integerDBType = builder.getDBTypeFactory().getDBLargeIntegerType();

        P = builder.createDatabaseRelation("P",
                "A", integerDBType, true,
                "B", integerDBType, true);

        attX = P.getAttribute(1).getID();
        attY = P.getAttribute(2).getID();

        f1 = IQ_FACTORY.createExtensionalDataNode(P, ImmutableMap.of(0, x, 1, y));

        qaTx = new QualifiedAttributeID(P.getID(), attX);
        qaTy = new QualifiedAttributeID(P.getID(), attY);
        qaNx = new QualifiedAttributeID(null, attX);
        qaNy = new QualifiedAttributeID(null, attY);

        re1 = new RAExpression(ImmutableList.of(f1),
                ImmutableList.of(),
                RAExpressionAttributesOperations.create(
                        ImmutableMap.of(attX, x, attY, y),
                        P.getID(), P.getAllIDs()));

        u = TERM_FACTORY.getVariable("u");
        v = TERM_FACTORY.getVariable("v");

        DatabaseRelationDefinition Q = builder.createDatabaseRelation("Q",
            "A", integerDBType, true,
            "C", integerDBType, true);

        QuotedID attu = Q.getAttribute(1).getID();
        QuotedID attv = Q.getAttribute(2).getID();

        f2 = IQ_FACTORY.createExtensionalDataNode(Q, ImmutableMap.of(0, u, 1, v));

        qaTu = new QualifiedAttributeID(Q.getID(), attu);
        qaTv = new QualifiedAttributeID(Q.getID(), attv);
        qaNv = new QualifiedAttributeID(null, attv);

        re2 = new RAExpression(ImmutableList.of(f2),
                ImmutableList.of(),
                RAExpressionAttributesOperations.create(
                        ImmutableMap.of(attu, u, attv, v),
                        Q.getID(), Q.getAllIDs()));

        Variable w = TERM_FACTORY.getVariable("u");
        Variable z = TERM_FACTORY.getVariable("v");

        ExtensionalDataNode f3 = IQ_FACTORY.createExtensionalDataNode(Q, ImmutableMap.of(0, w, 1, z));

        RelationID table3 = MDFAC.createRelationID(null, "R");
        QuotedID attW = MDFAC.createAttributeID("A");
        QuotedID attZ = MDFAC.createAttributeID("B");


        // This is used to simulate an ambiguity during the operation of natural join
        re3 = new RAExpression(
                ImmutableList.of(f3),
                ImmutableList.of(),
                RAExpressionAttributesOperations.create(ImmutableMap.of(attW, w, attZ, z), table3, ImmutableSet.of(table3)));

        eq = TERM_FACTORY.getNotYetTypedEquality(x, u);

        onExpression = new EqualsTo();
        onExpression.setLeftExpression(new Column(new Table("P"), "A"));
        onExpression.setRightExpression(new Column(new Table("Q"), "A"));

        // this relation contains just a common attribute with the RAExpression "re1"
        // and it is used to simulate an exception during the operations of:
        // "cross join" and "join on" and "natural join"
        re1_1 = new RAExpression(ImmutableList.of(f2),
                ImmutableList.of(),
                RAExpressionAttributesOperations.create(ImmutableMap.of(attX, x), P.getID(), P.getAllIDs()));

        System.out.println("****************************************************");
    }

    @Test
    public void cross_join_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re2);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression = ops.crossJoin(re1, re2);
        System.out.println(relationalExpression);

        crossJoinAndJoinOnCommonAsserts(relationalExpression);
        assertTrue(relationalExpression.getFilterAtoms().isEmpty());
    }

    @Test(expected = IllegalJoinException.class)
    public void cross_join_exception_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re1_1);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        ops.crossJoin(re1, re1_1);
    }

    @Test
    public void join_on_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re2);
        System.out.println(eq);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression = ops.joinOn(re1, re2,
                attributes -> new ExpressionParser(MDFAC, CORE_SINGLETONS)
                        .parseBooleanExpression(onExpression,  attributes.asMap()));

        System.out.println(relationalExpression);

        crossJoinAndJoinOnCommonAsserts(relationalExpression);
        assertEquals(ImmutableList.of(eq), relationalExpression.getFilterAtoms());
    }

    @Test(expected = IllegalJoinException.class)
    public void join_on_exception_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re1_1);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        ops.joinOn(re1, re1_1,
                attributes -> new ExpressionParser(MDFAC, CORE_SINGLETONS)
                        .parseBooleanExpression(onExpression, attributes.asMap()));
    }

    @Test
    public void natural_join_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re2);
        System.out.println(eq);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression = ops.naturalJoin(re1, re2);
        System.out.println(relationalExpression);

        naturalUsingCommonAsserts(relationalExpression);
    }

    @Test(expected = IllegalJoinException.class)
    public void natural_join_exception_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re1_1);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression = ops.naturalJoin(re1, re1_1);
        System.out.println(relationalExpression);
    }

    @Test(expected = IllegalJoinException.class)
    public void natural_join_ambiguity_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re2);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression re = ops.joinOn(re1, re2,
                attributes -> new ExpressionParser(MDFAC, CORE_SINGLETONS)
                        .parseBooleanExpression(onExpression, attributes.asMap()));

        System.out.println(re);
        System.out.println(re3);

        ops.naturalJoin(re, re3);
    }

    @Test
    public void join_using_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re2);
        System.out.println(eq);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression =
                ops.joinUsing(re1, re2, ImmutableSet.of(MDFAC.createAttributeID("A")));

        System.out.println(relationalExpression);

        naturalUsingCommonAsserts(relationalExpression);
    }


    @Test(expected = IllegalJoinException.class)
    public void join_using_exception_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re1_1);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression = ops.joinUsing(re1, re1_1,
                ImmutableSet.of(MDFAC.createAttributeID("A")));
        System.out.println(relationalExpression);
    }

    @Test(expected = IllegalJoinException.class)
    public void join_using_no_commons_test() throws IllegalJoinException {

        RelationID Q = MDFAC.createRelationID(null, "Q");
        // a new relationId without any common attribute with the re1 is created to simulate an exception
        RAExpression re2 =  new RAExpression(ImmutableList.of(f2),
                ImmutableList.of(),
                RAExpressionAttributesOperations.create(
                        ImmutableMap.of(MDFAC.createAttributeID("C"), u,  MDFAC.createAttributeID("D"), v),
                        Q, ImmutableSet.of(Q)));

        System.out.println(re1);
        System.out.println(re2);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        ops.joinUsing(re1, re2, ImmutableSet.of(MDFAC.createAttributeID("A")));
    }

    @Test(expected = IllegalJoinException.class)
    public void join_using_ambiguity_test() throws IllegalJoinException {
        System.out.println(re1);
        System.out.println(re2);

        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression relationalExpression = ops.joinOn(re1, re2,
                attributes -> new ExpressionParser(MDFAC, CORE_SINGLETONS)
                        .parseBooleanExpression(onExpression, attributes.asMap()));

        System.out.println(relationalExpression);
        System.out.println(re3);

        ops.joinUsing(relationalExpression, re3, ImmutableSet.of(MDFAC.createAttributeID("A")));
    }


    @Test
    public void alias_test() {
        RelationID tableAlias = MDFAC.createRelationID(null, "S");
        QualifiedAttributeID qaAx = new QualifiedAttributeID(tableAlias, attX);
        QualifiedAttributeID qaAy = new QualifiedAttributeID(tableAlias, attY);

        System.out.println(re1);
        RAExpressionOperations ops = new RAExpressionOperations(TERM_FACTORY);
        RAExpression actual =  ops.withAlias(re1, tableAlias);
        System.out.println(actual);

        assertTrue(actual.getDataAtoms().contains(f1));

        assertEquals(ImmutableMap.of(qaNx, x, qaNy, y, qaAx, x, qaAy, y), actual.getAttributes().asMap());
    }

    @Test
    public void create_test() {
        RAExpression actual = new RAExpression(re1.getDataAtoms(),
                re1.getFilterAtoms(),
                RAExpressionAttributesOperations.create(ImmutableMap.of(attX, x, attY, y), P.getID(), P.getAllIDs()));
        System.out.println(actual);

        assertEquals(ImmutableMap.of(qaNx, x, qaNy, y, qaTx, x, qaTy, y), actual.getAttributes().asMap());
    }


    private void naturalUsingCommonAsserts(RAExpression relationalExpression) {
        assertTrue(relationalExpression.getDataAtoms().contains(f1));
        assertTrue(relationalExpression.getDataAtoms().contains(f2));
        assertEquals(ImmutableList.of(eq), relationalExpression.getFilterAtoms());

        assertEquals(ImmutableMap.of(qaNx, x, qaTy, y, qaNy, y, qaTv, v, qaNv, v), relationalExpression.getAttributes().asMap());
    }

    private void crossJoinAndJoinOnCommonAsserts(RAExpression relationalExpression ){
        assertTrue(relationalExpression.getDataAtoms().contains(f1));
        assertTrue(relationalExpression.getDataAtoms().contains(f2));

        assertEquals(ImmutableMap.builder()
                .put(qaTx, x)
                .put(qaTy, y)
                .put(qaNy, y)
                .put(qaTu, u)
                .put(qaTv, v)
                .put(qaNv, v).build(), relationalExpression.getAttributes().asMap());
    }
}
