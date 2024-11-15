package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    private final BidiMap<Integer, Term> dict = new DualHashBidiMap<>();

    private final Map<Integer, Map<Integer, Set<Integer>>> spo = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> pso = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> osp = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> sop = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> pos = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> ops = new HashMap<>();

    private long size;

    /**
     * Créer ou retourne une nouvelle indexe dans {@link RDFHexaStore#dict} pour un terme donné.
     *
     * @param term Term
     * @return Index
     */
    private int index(Term term) {
        // TODO: Utiliser une HashMap si le temps d'insertion n'est pas important.
        // BidiMap permet de faire ce get en O(1) mais on passe juste à O(n) si on utilise une Map.
        return dict.inverseBidiMap().computeIfAbsent(term, k -> dict.size() + 1);
    }

    private Term term(int index) {
        return dict.get(index);
    }

    @Override
    public boolean add(RDFAtom atom) {
        return add(
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
    public boolean add(int s, int p, int o) {
        var r = add(spo, s, p, o)
                && add(pso, p, s, o)
                && add(osp, o, s, p)
                && add(sop, s, o, p)
                && add(pos, p, o, s)
                && add(ops, o, p, s);
        if (r) size++;
        return r;
    }

    /**
     * Insère le triplet dans l'indexing passée en paramètre.
     *
     * @param indexes L'indexing
     * @param a Index du premier élément
     * @param b Index du deuxième élémént
     * @param c Index du troisième élément
     */
    private boolean add(Map<Integer, Map<Integer, Set<Integer>>> indexes,
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
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        throw new NotImplementedException();
    }

    @Override
    public Collection<Atom> getAtoms() {
        var atoms = new ArrayList<Atom>((int)size());
        for (var sI: spo.keySet()) {
            for (var pI: spo.get(sI).keySet()) {
                for (var oI: spo.get(sI).get(pI)) {
                    atoms.add(new RDFAtom(term(sI), term(pI), term(oI)));
                }
            }
        }
        return atoms;
    }

}
