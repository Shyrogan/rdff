package qengine.storage;

import com.google.common.collect.Streams;
import fr.boreal.model.logicalElements.api.Literal;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.junit.Before;
import org.junit.Test;
import qengine.model.RDFAtom;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class RDFMatcherTest {

    private static final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("subject1");
    private static final Literal<String> PREDICATE_1 = SameObjectTermFactory.instance().createOrGetLiteral("predicate1");
    private static final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("object1");
    private static final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
    private static final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");
    private static final Variable VAR_Z = SameObjectTermFactory.instance().createOrGetVariable("?z");

    private static final RDFHexaStore store = new RDFHexaStore();

    @Before
    public void setupAtoms() {
        var atom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        store.add(atom);
    }

    @Test
    public void testVarLitLit() {
        var query = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        var optMatcher = RDFMatcher.match(query);
        assertTrue(optMatcher.isPresent(), "(VAR, LIT, LIT) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.VAR_LIT_LIT, matcher, "(VAR, LIT, LIT) matcher expected");

        var subs = Streams.stream(matcher.substitution(store, query))
                .collect(Collectors.toSet());
        assertEquals(1, subs.size(), "Expected 1 substitution");
        assertTrue(subs.contains(new SubstitutionImpl(Map.of(VAR_X, SUBJECT_1))));
    }

    @Test
    public void testVarVarLit() {
        var query = new RDFAtom(VAR_X, VAR_Y, OBJECT_1);
        var optMatcher = RDFMatcher.match(query);
        assertTrue(optMatcher.isPresent(), "(VAR, VAR, LIT) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.VAR_VAR_LIT, matcher, "(VAR, VAR, LIT) matcher expected");

        var subs = Streams.stream(matcher.substitution(store, query))
                .collect(Collectors.toSet());
        assertEquals(1, subs.size(), "Expected 1 substitution");
        assertTrue(subs.contains(new SubstitutionImpl(Map.of(
                VAR_X, SUBJECT_1,
                VAR_Y, PREDICATE_1
        ))));
    }

    @Test
    public void testVarVarVar() {
        var query = new RDFAtom(VAR_X, VAR_Y, VAR_Z);
        var optMatcher = RDFMatcher.match(query);
        assertTrue(optMatcher.isPresent(), "(VAR, VAR, VAR) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.VAR_VAR_VAR, matcher, "(VAR, VAR, VAR) matcher expected");

        var subs = Streams.stream(matcher.substitution(store, query))
                .collect(Collectors.toSet());
        assertEquals(1, subs.size(), "Expected 1 substitution");
        assertTrue(subs.contains(new SubstitutionImpl(Map.of(
                VAR_X, SUBJECT_1,
                VAR_Y, PREDICATE_1,
                VAR_Z, OBJECT_1
        ))));
    }

    @Test
    public void testLitVarVar() {
        var query = new RDFAtom(SUBJECT_1, VAR_Y, VAR_Z);
        var optMatcher = RDFMatcher.match(query);
        assertTrue(optMatcher.isPresent(), "(LIT, VAR, VAR) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.LIT_VAR_VAR, matcher, "(LIT, VAR, VAR) matcher expected");

        var subs = Streams.stream(matcher.substitution(store, query))
                .collect(Collectors.toSet());
        assertEquals(1, subs.size(), "Expected 1 substitution");
        assertTrue(subs.contains(new SubstitutionImpl(Map.of(
                VAR_Y, PREDICATE_1,
                VAR_Z, OBJECT_1
        ))));
    }

    @Test
    public void testLitLitVar() {
        var query = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_Z);
        var optMatcher = RDFMatcher.match(query);
        assertTrue(optMatcher.isPresent(), "(LIT, LIT, VAR) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.LIT_LIT_VAR, matcher, "(LIT, LIT, VAR) matcher expected");

        var subs = Streams.stream(matcher.substitution(store, query))
                .collect(Collectors.toSet());
        assertEquals(1, subs.size(), "Expected 1 substitution");
        assertTrue(subs.contains(new SubstitutionImpl(Map.of(
                VAR_Z, OBJECT_1
        ))));
    }

    @Test
    public void testLitVarLit() {
        var query = new RDFAtom(SUBJECT_1, VAR_Y, OBJECT_1);
        var optMatcher = RDFMatcher.match(query);
        assertTrue(optMatcher.isPresent(), "(LIT, VAR, LIT) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.LIT_VAR_LIT, matcher, "(LIT, VAR, LIT) matcher expected");

        var subs = Streams.stream(matcher.substitution(store, query))
                .collect(Collectors.toSet());
        assertEquals(1, subs.size(), "Expected 1 substitution");
        assertTrue(subs.contains(new SubstitutionImpl(Map.of(
                VAR_Y, PREDICATE_1
        ))));
    }

    @Test
    public void testLitLitLit() {
        var atom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        var optMatcher = RDFMatcher.match(atom);
        assertTrue(optMatcher.isPresent(), "(LIT, LIT, LIT) atom should have a matcher.");
        var matcher = optMatcher.get();
        assertEquals(RDFMatcher.LIT_LIT_LIT, matcher, "(LIT, LIT, LIT) matcher expected");

        for (Iterator<Substitution> it = matcher.substitution(store, atom); it.hasNext();) {
            fail("There shouldn't be a substitution if there are no variables");
        }
    }


}
