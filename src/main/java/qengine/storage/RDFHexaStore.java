package qengine.storage;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.logicalElements.api.Term;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.stream.Collectors;

import static fr.lirmm.graphik.util.stream.Iterators.emptyIterator;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    final BidiMap<Integer, Term> dict = new DualHashBidiMap<>();

    final Map<Integer, Map<Integer, Set<Integer>>> spo = new HashMap<>();
    final Map<Integer, Map<Integer, Set<Integer>>> pso = new HashMap<>();
    final Map<Integer, Map<Integer, Set<Integer>>> osp = new HashMap<>();
    final Map<Integer, Map<Integer, Set<Integer>>> sop = new HashMap<>();
    final Map<Integer, Map<Integer, Set<Integer>>> pos = new HashMap<>();
    final Map<Integer, Map<Integer, Set<Integer>>> ops = new HashMap<>();

    private long size;

    /**
     * Créer ou retourne une nouvelle indexe dans {@link RDFHexaStore#dict} pour un terme donné.
     *
     * @param term Term
     * @return Index
     */
    int index(Term term) {
        // TODO: Utiliser une HashMap si le temps d'insertion n'est pas important.
        // BidiMap permet de faire ce get en O(1) mais on passe juste à O(n) si on utilise une Map.
        return dict.inverseBidiMap().computeIfAbsent(term, k -> dict.size() + 1);
    }

    private Term term(int index) {
        return dict.get(index);
    }

    @Override
    public boolean add(RDFAtom atom) {
        return addIndex(
                index(atom.getTripleSubject()),
                index(atom.getTriplePredicate()),
                index(atom.getTripleObject())
        );
    }

    /**
     * Insère le triplet d'indexes à l'ensemble des dictionnaires d'indexes.
     *
     * @param s L'indexe du sujet
     * @param o L'indexe du prédicat
     * @param p L'indexe de l'objet
     */
    public boolean addIndex(int s, int p, int o) {
        var r = addToStore(spo, s, p, o)
                && addToStore(pso, p, s, o)
                && addToStore(osp, o, s, p)
                && addToStore(sop, s, o, p)
                && addToStore(pos, p, o, s)
                && addToStore(ops, o, p, s);
        if (r) size++;
        return r;
    }

    /**
     * Insère le triplet dans l'indexing passée en paramètre.
     *
     * @param indexes L'indexing
     * @param a       Index du premier élément
     * @param b       Index du deuxième élémént
     * @param c       Index du troisième élément
     */
    private boolean addToStore(Map<Integer, Map<Integer, Set<Integer>>> indexes,
                               int a, int b, int c) {
        var x = indexes.computeIfAbsent(a, k -> new HashMap<>());
        var y = x.computeIfAbsent(b, k -> new HashSet<>());

        return y.add(c);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        for (var matcher : RDFMatcher.values()) {
            if (matcher.matches(atom)) {
                return matcher.substitution(this, atom);
            }
        }
        return emptyIterator();
    }

    private long estimateMatchNumbers(RDFAtom atom) {
        return ops
                .getOrDefault(index(atom.getTripleObject()), new HashMap<>())
                .getOrDefault(index(atom.getTriplePredicate()), new HashSet<>())
                .size();
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        List<RDFAtom> queryAtoms = new ArrayList<>(q.getRdfAtoms());

        var smallestSubstitutionOpt = queryAtoms.stream()
                .min((q1, q2) -> Math.toIntExact(estimateMatchNumbers(q1) - estimateMatchNumbers(q2)));
        if (!smallestSubstitutionOpt.isPresent()) {
            return emptyIterator();
        }
        var smallestQuery = smallestSubstitutionOpt.get();
        queryAtoms.remove(smallestQuery);

        var subs = Streams.stream(match(smallestQuery))
                .filter(sub -> queryAtoms.stream().allMatch(query -> Streams.stream(match(query))
                        .anyMatch(sub::equals)));

        return subs.iterator();

        /** Étape 1 : Trouver le triplet avec le moins de correspondances
        RDFAtom firstToProcess = null;
        long minEstimate = Long.MAX_VALUE;

        for (RDFAtom queryAtom : queryAtoms) {
            long estimate = estimateMatchNumbers(queryAtom);
            if (estimate < minEstimate) {
                minEstimate = estimate;
                firstToProcess = queryAtom;
            }
        }

        if (firstToProcess == null) {
            return Collections.emptyIterator();
        }

        // Étape 2 : Effectuer le premier match
        Iterator<Substitution> initialMatches = match(firstToProcess);
        List<Substitution> validSubstitutions = new ArrayList<>();

        // Étape 3 : Filtrer les substitutions en fonction des autres triplets
        while (initialMatches.hasNext()) {
            Substitution currentSubstitution = initialMatches.next();
            boolean isValid = true;

            for (RDFAtom queryAtom : queryAtoms) {
                if (queryAtom.equals(firstToProcess)) {
                    continue; // Ne pas retraiter le triplet déjà matché
                }

                // Appliquer la substitution actuelle au triplet
                RDFAtom substitutedAtom = applySubstitutionToAtom(queryAtom, currentSubstitution);

                // Vérifier si ce triplet substitué a des correspondances
                Iterator<Substitution> matches = match(substitutedAtom);
                if (!matches.hasNext()) {
                    isValid = false;
                    break;
                }
            }
            if (isValid) {
                validSubstitutions.add(currentSubstitution);
            }
        }

        return validSubstitutions.iterator();**/
    }

    @Override
    public Collection<Atom> getAtoms() {
        var atoms = new ArrayList<Atom>((int) size());
        for (var sI : spo.keySet()) {
            for (var pI : spo.get(sI).keySet()) {
                for (var oI : spo.get(sI).get(pI)) {
                    atoms.add(new RDFAtom(term(sI), term(pI), term(oI)));
                }
            }
        }
        return atoms;
    }

    private RDFAtom applySubstitutionToAtom(RDFAtom atom, Substitution substitution) {
        Term substitutedSubject = substitution.createImageOf(atom.getTripleSubject());
        Term substitutedPredicate = substitution.createImageOf(atom.getTriplePredicate());
        Term substitutedObject = substitution.createImageOf(atom.getTripleObject());

        return new RDFAtom(substitutedSubject, substitutedPredicate, substitutedObject);
    }
}
