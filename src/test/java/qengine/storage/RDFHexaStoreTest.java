package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.parser.RDFAtomParser;
import qengine.storage.RDFHexaStore;
import org.junit.jupiter.api.Test;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe {@link RDFHexaStore}.
 */
public class RDFHexaStoreTest {
    private static final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("subject1");
    private static final Literal<String> PREDICATE_1 = SameObjectTermFactory.instance().createOrGetLiteral("predicate1");
    private static final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("object1");
    private static final Literal<String> SUBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("subject2");
    private static final Literal<String> PREDICATE_2 = SameObjectTermFactory.instance().createOrGetLiteral("predicate2");
    private static final Literal<String> OBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("object2");
    private static final Literal<String> OBJECT_3 = SameObjectTermFactory.instance().createOrGetLiteral("object3");
    private static final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
    private static final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");


    @Test
    public void testAddAllRDFAtoms() {
        RDFHexaStore store = new RDFHexaStore();

        // Version stream
        // Ajouter plusieurs RDFAtom
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        Set<RDFAtom> rdfAtoms = Set.of(rdfAtom1, rdfAtom2);

        assertTrue(store.addAll(rdfAtoms.stream()), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");

        // Version collection
        store = new RDFHexaStore();
        assertTrue(store.addAll(rdfAtoms), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");
    }

    @Test
    public void testAddRDFAtom() {
        RDFHexaStore store = new RDFHexaStore();

        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        store.add(rdfAtom1);
        assertTrue(store.getAtoms().contains(rdfAtom1), "La base devrait contenir l'atome 1");

        store.add(rdfAtom2);
        assertTrue(store.getAtoms().contains(rdfAtom2), "La base devrait contenir l'atome 2");
    }

    @Test
    public void testAddDuplicateAtom() {
        RDFHexaStore store = new RDFHexaStore();

        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        store.add(rdfAtom1);
        assertTrue(store.getAtoms().contains(rdfAtom1), "La base devrait contenir l'atome 1.");

        store.add(rdfAtom2);
        assertTrue(store.getAtoms().contains(rdfAtom2), "La base devrait contenir l'atome 2.");

        store.add(rdfAtom2);
        assertTrue(store.getAtoms().contains(rdfAtom2), "La base devrait contenir l'atome 2.");
    }

    @Test
    public void testSize() {
        RDFHexaStore store = new RDFHexaStore();

        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        store.add(rdfAtom1);
        assertEquals(1, store.size(), "La base n'a pas la taille attendue");

        store.add(rdfAtom2);
        assertEquals(2, store.size(), "La base n'a pas la taille attendue");
    }

    @Test
    public void testMatchAtom() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1)); // RDFAtom(subject1, triple, object1)
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2)); // RDFAtom(subject2, triple, object2)
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3)); // RDFAtom(subject1, triple, object3)

        // Case 1
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_X); // RDFAtom(subject1, predicate1, X)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, OBJECT_1);
        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, OBJECT_3);

        assertEquals(2, matchedList.size(), "There should be two matched RDFAtoms");
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
    }

    @Test
    public void testMatchStarQuery() {

        RDFHexaStore store = new RDFHexaStore();

        // Ajouter des triplets RDF
        store.add(new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(VAR_X, PREDICATE_2, OBJECT_2));

        // Construire une requête en étoile avec VAR_X comme sujet central
        List<RDFAtom> starPattern = List.of(
                new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1),
                new RDFAtom(VAR_X, PREDICATE_2, OBJECT_2)
        );
        List<Variable> answerVariables = List.of((Variable) VAR_X);

        StarQuery starQuery = new StarQuery("Test Star Query", starPattern, answerVariables);

        // Effectuer le match
        Iterator<Substitution> matches = store.match(starQuery);
        List<Substitution> matchedList = new ArrayList<>();
        matches.forEachRemaining(matchedList::add);

        // Vérifier les résultats attendus
        Substitution expectedMatch = new SubstitutionImpl();
        expectedMatch.add(VAR_X, VAR_X); // VAR_X est son propre sujet central (unique)

        assertEquals(1, matchedList.size(), "Une seule correspondance doit être trouvée.");
        assertTrue(matchedList.contains(expectedMatch), "La correspondance attendue est manquante.");
    }

}
