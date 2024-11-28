package qengine.storage;

import com.google.common.collect.Iterators;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import qengine.model.RDFAtom;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyIterator;

public enum RDFMatcher {

    VAR_LIT_LIT(
            atom -> atom.getTripleSubject().isVariable() && !atom.getTriplePredicate().isVariable()
                    && !atom.getTripleObject().isVariable(),
            (store, atom) -> {
                var results = store.pos.getOrDefault(store.index(atom.getTriplePredicate()), new HashMap<>())
                        .getOrDefault(store.index(atom.getTripleObject()), new HashSet<>())
                        .stream()
                        .map(i -> (Substitution) new SubstitutionImpl(Map.of((Variable) atom.getTripleSubject(), store.dict.get(i))))
                        .collect(Collectors.toSet());
                return results.iterator();
            }),

    VAR_VAR_LIT(
            atom -> atom.getTripleSubject().isVariable() && atom.getTriplePredicate().isVariable()
                    && !atom.getTripleObject().isVariable(),
            (store, atom) -> {
                var results = store.ops.getOrDefault(store.index(atom.getTripleObject()), new HashMap<>())
                        .entrySet().stream()
                        .flatMap(entry -> entry.getValue().stream()
                                .map(i -> (Substitution) new SubstitutionImpl(Map.of(
                                        (Variable) atom.getTriplePredicate(), store.dict.get(entry.getKey()),
                                        (Variable) atom.getTripleSubject(), store.dict.get(i)))))
                        .collect(Collectors.toSet());
                return results.iterator();
            }),

    VAR_VAR_VAR(
            atom -> atom.getTripleSubject().isVariable() && atom.getTriplePredicate().isVariable()
                    && atom.getTripleObject().isVariable(),
            (store, atom) -> {
                var results = store.spo.entrySet().stream()
                        .flatMap(subjectEntry -> subjectEntry.getValue().entrySet().stream()
                                .flatMap(predicateEntry -> predicateEntry.getValue().stream()
                                        .map(objectId -> (Substitution) new SubstitutionImpl(Map.of(
                                                (Variable) atom.getTripleSubject(), store.dict.get(subjectEntry.getKey()),
                                                (Variable) atom.getTriplePredicate(), store.dict.get(predicateEntry.getKey()),
                                                (Variable) atom.getTripleObject(), store.dict.get(objectId))))))
                        .collect(Collectors.toSet());
                return results.iterator();
            }),

    LIT_VAR_VAR(
            atom -> !atom.getTripleSubject().isVariable() && atom.getTriplePredicate().isVariable()
                    && atom.getTripleObject().isVariable(),
            (store, atom) -> {
                var results = store.spo.getOrDefault(store.index(atom.getTripleSubject()), new HashMap<>())
                        .entrySet().stream()
                        .flatMap(predicateEntry -> predicateEntry.getValue().stream()
                                .map(objectId -> (Substitution) new SubstitutionImpl(Map.of(
                                        (Variable) atom.getTriplePredicate(), store.dict.get(predicateEntry.getKey()),
                                        (Variable) atom.getTripleObject(), store.dict.get(objectId)))))
                        .collect(Collectors.toSet());
                return results.iterator();
            }),

    LIT_LIT_VAR(
            atom -> !atom.getTripleSubject().isVariable() && !atom.getTriplePredicate().isVariable()
                    && atom.getTripleObject().isVariable(),
            (store, atom) -> {
                var results = store.spo.getOrDefault(store.index(atom.getTripleSubject()), new HashMap<>())
                        .getOrDefault(store.index(atom.getTriplePredicate()), new HashSet<>())
                        .stream()
                        .map(objectId -> (Substitution) new SubstitutionImpl(Map.of(
                                (Variable) atom.getTripleObject(), store.dict.get(objectId))))
                        .collect(Collectors.toSet());
                return results.iterator();
            }),

    LIT_VAR_LIT(
            atom -> !atom.getTripleSubject().isVariable() && atom.getTriplePredicate().isVariable()
                    && !atom.getTripleObject().isVariable(),
            (store, atom) -> {
                var results = store.spo.getOrDefault(store.index(atom.getTripleSubject()), new HashMap<>())
                        .entrySet().stream()
                        .filter(entry -> entry.getValue().contains(store.index(atom.getTripleObject())))
                        .map(entry -> (Substitution) new SubstitutionImpl(Map.of(
                                (Variable) atom.getTriplePredicate(), store.dict.get(entry.getKey()))))
                        .collect(Collectors.toSet());
                return results.iterator();
            }),
    LIT_LIT_LIT(
            atom -> !atom.getTripleSubject().isVariable() && !atom.getTriplePredicate().isVariable()
                    && !atom.getTripleObject().isVariable(),
            (store, atom) -> emptyIterator());

    public static Optional<RDFMatcher> match(RDFAtom atom) {
        for (var matcher: RDFMatcher.values()) {
            if (matcher.matches(atom)) {
                return Optional.of(matcher);
            }
        }
        return Optional.empty();
    }

    private final Predicate<RDFAtom> predicate;
    private final BiFunction<RDFHexaStore, RDFAtom, Iterator<Substitution>> extractor;

    RDFMatcher(Predicate<RDFAtom> predicate, BiFunction<RDFHexaStore, RDFAtom, Iterator<Substitution>> extractor) {
        this.predicate = predicate;
        this.extractor = extractor;
    }

    public boolean matches(RDFAtom atom) {
        return predicate.test(atom);
    }

    public Iterator<Substitution> substitution(RDFHexaStore hexaStore, RDFAtom atom) {
        return extractor.apply(hexaStore, atom);
    }
}

