package concurentStore;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.storage.RDFStorage;

import java.util.*;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class ConcuRDFHexaStore implements RDFStorage {
    // Déclaration des six index
    private final Map<Integer, Map<Integer, Set<Integer>>> spo = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> sop = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> pso = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> pos = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> osp = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> ops = new HashMap<>();
    private final ConcuRDFDictionary dictionary = new ConcuRDFDictionary();

    /**
     * Ajoute un RDFAtom aux six index.
     *
     * @param atom le RDFAtom à ajouter
     * @return true si l'ajout est réussi
     */
    @Override
    public boolean add(RDFAtom atom) {
        // Encode les termes du triplet RDF en identifiants
        int subjectId = dictionary.encode(atom.getTripleSubject());
        int predicateId = dictionary.encode(atom.getTriplePredicate());
        int objectId = dictionary.encode(atom.getTripleObject());

        // Affiche le triplet encodé
//        System.out.println("Triplet encodé : (" + subjectId + ", " + predicateId + ", " + objectId + ")");

        // Ajoute le triplet aux six index
        addToIndex(spo, subjectId, predicateId, objectId);
        addToIndex(sop, subjectId, objectId, predicateId);
        addToIndex(pso, predicateId, subjectId, objectId);
        addToIndex(pos, predicateId, objectId, subjectId);
        addToIndex(osp, objectId, subjectId, predicateId);
        addToIndex(ops, objectId, predicateId, subjectId);

        return true;
    }

    /**
     * Ajoute un triplet à un index spécifique.
     *
     * @param index  l'index où ajouter le triplet
     * @param first  le premier terme du triplet
     * @param second le deuxième terme du triplet
     * @param third  le troisième terme du triplet
     */
    private void addToIndex(Map<Integer, Map<Integer, Set<Integer>>> index, int first, int second, int third) {
        // Ajoute le triplet à l'index
        index.computeIfAbsent(first, k -> new HashMap<>())
                .computeIfAbsent(second, k -> new HashSet<>())
                .add(third);

        // Affiche l'index après l'ajout
//        System.out.println("Index après ajout :");
//        index.forEach((key, value) -> System.out.println(" " + key + " -> " + value));
    }

    /**
     * Affiche tous les index.
     */
    public void printAllIndexes() {
        System.out.println("Index SPO :");
        printIndex(spo);
        System.out.println("Index SOP :");
        printIndex(sop);
        System.out.println("Index PSO :");
        printIndex(pso);
        System.out.println("Index POS :");
        printIndex(pos);
        System.out.println("Index OSP :");
        printIndex(osp);
        System.out.println("Index OPS :");
        printIndex(ops);
    }

    /**
     * Efface tous les index.
     */
    public void clearAllIndexes() {
        spo.clear();
        sop.clear();
        pso.clear();
        pos.clear();
        osp.clear();
        ops.clear();

        System.out.println("Tous les index ont été effacés.");
    }

    /**
     * Affiche un index spécifique.
     *
     * @param index l'index à afficher
     */
    private void printIndex(Map<Integer, Map<Integer, Set<Integer>>> index) {
        index.forEach((key, value) -> {
            System.out.println(" " + key + " -> " + value);
        });
    }

    /**
     * Retourne la taille totale de l'HexaStore.
     *
     * @return la taille totale
     */
    @Override
    public long size() {
        return spo.values().stream()
                .flatMap(map -> map.values().stream())
                .mapToLong(Set::size)
                .sum();
    }

    /**
     * Retourne un itérateur de substitutions pour matcher un RDFAtom.
     *
     * @param atom le RDFAtom à matcher
     * @return un itérateur de substitutions
     */
    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        List<Substitution> substitutions = new ArrayList<>();

        // Encode les termes du triplet RDF en identifiants, ou les définit à -1 s'ils sont des variables
        int subjectId = atom.getTripleSubject().isVariable() ? -1 : dictionary.encode(atom.getTripleSubject());
        int predicateId = atom.getTriplePredicate().isVariable() ? -1 : dictionary.encode(atom.getTriplePredicate());
        int objectId = atom.getTripleObject().isVariable() ? -1 : dictionary.encode(atom.getTripleObject());

        // Tous les termes sont des littéraux
        if (subjectId != -1 && predicateId != -1 && objectId != -1) {
            if (spo.containsKey(subjectId) && spo.get(subjectId).containsKey(predicateId) && spo.get(subjectId).get(predicateId).contains(objectId)) {
                substitutions.add(new SubstitutionImpl(Collections.emptyMap())); // Ajoute une substitution vide
            }
        }
        // Sujet et prédicat sont des littéraux, objet est une variable
        else if (subjectId != -1 && predicateId != -1) {
            if (spo.containsKey(subjectId) && spo.get(subjectId).containsKey(predicateId)) {
                for (int objId : spo.get(subjectId).get(predicateId)) { // Parcourt les objets correspondants
                    SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                    substitution.add((Variable) atom.getTripleObject(), dictionary.decode(objId)); // Ajoute l'objet à la substitution
                    substitutions.add(substitution); // Ajoute la substitution à la liste
                }
            }
        }
        // Sujet et objet sont des littéraux, prédicat est une variable
        else if (subjectId != -1 && objectId != -1) {
            if (sop.containsKey(subjectId) && sop.get(subjectId).containsKey(objectId)) {
                for (int predId : sop.get(subjectId).get(objectId)) { // Parcourt les prédicats correspondants
                    SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                    substitution.add((Variable) atom.getTriplePredicate(), dictionary.decode(predId)); // Ajoute le prédicat à la substitution
                    substitutions.add(substitution); // Ajoute la substitution à la liste
                }
            }
        }
        // Prédicat et objet sont des littéraux, sujet est une variable
        else if (predicateId != -1 && objectId != -1) {
            if (pos.containsKey(predicateId) && pos.get(predicateId).containsKey(objectId)) {
                for (int subjId : pos.get(predicateId).get(objectId)) { // Parcourt les sujets correspondants
                    SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                    substitution.add((Variable) atom.getTripleSubject(), dictionary.decode(subjId)); // Ajoute le sujet à la substitution
                    substitutions.add(substitution); // Ajoute la substitution à la liste
                }
            }
        }
        // Sujet est un littéral, prédicat et objet sont des variables
        else if (subjectId != -1) {
            if (spo.containsKey(subjectId)) {
                for (Map.Entry<Integer, Set<Integer>> predEntry : spo.get(subjectId).entrySet()) {
                    for (int objId : predEntry.getValue()) { // Parcourt les objets correspondants
                        SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                        substitution.add((Variable) atom.getTriplePredicate(), dictionary.decode(predEntry.getKey())); // Ajoute le prédicat à la substitution
                        substitution.add((Variable) atom.getTripleObject(), dictionary.decode(objId)); // Ajoute l'objet à la substitution
                        substitutions.add(substitution); // Ajoute la substitution à la liste
                    }
                }
            }
        }
        // Prédicat est un littéral, sujet et objet sont des variables
        else if (predicateId != -1) {
            if (pso.containsKey(predicateId)) {
                for (Map.Entry<Integer, Set<Integer>> subjEntry : pso.get(predicateId).entrySet()) {
                    for (int objId : subjEntry.getValue()) { // Parcourt les objets correspondants
                        SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                        substitution.add((Variable) atom.getTripleSubject(), dictionary.decode(subjEntry.getKey())); // Ajoute le sujet à la substitution
                        substitution.add((Variable) atom.getTripleObject(), dictionary.decode(objId)); // Ajoute l'objet à la substitution
                        substitutions.add(substitution); // Ajoute la substitution à la liste
                    }
                }
            }
        }
        // Objet est un littéral, sujet et prédicat sont des variables
        else if (objectId != -1) {
            if (osp.containsKey(objectId)) {
                for (Map.Entry<Integer, Set<Integer>> subjEntry : osp.get(objectId).entrySet()) {
                    for (int predId : subjEntry.getValue()) { // Parcourt les prédicats correspondants
                        SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                        substitution.add((Variable) atom.getTripleSubject(), dictionary.decode(subjEntry.getKey())); // Ajoute le sujet à la substitution
                        substitution.add((Variable) atom.getTriplePredicate(), dictionary.decode(predId)); // Ajoute le prédicat à la substitution
                        substitutions.add(substitution); // Ajoute la substitution à la liste
                    }
                }
            }
        }
        // Tous les termes sont des variables
        else {
            for (Map.Entry<Integer, Map<Integer, Set<Integer>>> subjEntry : spo.entrySet()) {
                for (Map.Entry<Integer, Set<Integer>> predEntry : subjEntry.getValue().entrySet()) {
                    for (int objId : predEntry.getValue()) { // Parcourt les objets correspondants
                        SubstitutionImpl substitution = new SubstitutionImpl(); // Crée une substitution
                        substitution.add((Variable) atom.getTripleSubject(), dictionary.decode(subjEntry.getKey())); // Ajoute le sujet à la substitution
                        substitution.add((Variable) atom.getTriplePredicate(), dictionary.decode(predEntry.getKey())); // Ajoute le prédicat à la substitution
                        substitution.add((Variable) atom.getTripleObject(), dictionary.decode(objId)); // Ajoute l'objet à la substitution
                        substitutions.add(substitution); // Ajoute la substitution à la liste
                    }
                }
            }
        }

        return substitutions.iterator();
    }

    /**
     * Matcher une StarQuery avec les triplets RDF stockés.
     *
     * @param q la StarQuery à matcher
     * @return un itérateur de substitutions
     */
    @Override
    public Iterator<Substitution> match(StarQuery q) {
        // Ensemble pour stocker les résultats finaux (intersection des substitutions entre tous les triplets)
        Set<Substitution> resultSet = null; // Initialisé à null pour gérer le premier triplet séparément

        // Parcourir chaque triplet RDF de la requête
        for (RDFAtom atom : q.getRdfAtoms()) {
            // Obtenir les substitutions possibles pour ce triplet
            Iterator<Substitution> atomSubstitutions = match(atom);

            // Créer un ensemble temporaire pour les substitutions du triplet courant
            Set<Substitution> currentSubstitutions = new HashSet<>();

            // Ajouter toutes les substitutions possibles du triplet courant à l'ensemble temporaire
            atomSubstitutions.forEachRemaining(currentSubstitutions::add);

            // Si le triplet courant n'a aucune correspondance, la requête entière échoue
            if (currentSubstitutions.isEmpty()) {
                return Collections.emptyIterator(); // Retourner un itérateur vide
            }

            // Initialiser resultSet avec les substitutions du premier triplet traité
            if (resultSet == null) {
                resultSet = currentSubstitutions;
            } else {
                // Faire l'intersection des résultats existants avec les substitutions actuelles
                resultSet.retainAll(currentSubstitutions);
            }

            // Si l'intersection devient vide, on peut arrêter plus tôt (aucun résultat possible)
            if (resultSet.isEmpty()) {
                return Collections.emptyIterator();
            }
        }

        // Si aucun triplet n'a été traité (par exemple, StarQuery vide), retourner un itérateur vide
        if (resultSet == null) {
            return Collections.emptyIterator();
        }

        // Retourner les résultats finaux sous forme d'itérateur
        return resultSet.iterator();
    }


    /**
     * Décode un identifiant en terme.
     *
     * @param index l'identifiant à décoder
     * @return le terme décodé
     */
    private Term term(int index) {
        return dictionary.decode(index);
    }

    /**
     * Retourne tous les atomes RDF stockés.
     *
     * @return une collection d'atomes RDF
     */
    @Override
    public Collection<Atom> getAtoms() {
        var atoms = new ArrayList<Atom>();
        // Parcourt l'index SPO pour récupérer tous les atomes RDF
        spo.forEach((s, map) -> map.forEach((p, set) -> set.forEach(o -> {
            atoms.add(new RDFAtom(term(s), term(p), term(o)));
        })));
        return atoms;
    }
}