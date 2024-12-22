package qengine.benchmark;

import concurentStore.ConcuRDFHexaStore;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import qengine.storage.RDFHexaStore;

import java.io.*;

public class QueryEngineBenchmark {
    private static final String WORKING_DIR = "data/";
    private static final String DATASET_500K = WORKING_DIR + "500k/jeu500K.nt";
    private static final String DATASET_2M = WORKING_DIR + "2M/jeu2M.nt";
    private static final String QUERY_500k = WORKING_DIR + "500k/queries.queryset";
    private static final String QUERY_2M = WORKING_DIR + "2M/queries.queryset";





    public static void main(String[] args) {
        var benchmarker = new Benchmarker<>(new ConcuRDFHexaStore())  // RDFHexaStore() |  ConcuRDFHexaStore() | new SimpleInMemoryGraphStore()
                // On charge les données et on les ajoute au store
                .loadDataSet(new File(DATASET_500K), ConcuRDFHexaStore::add)
                // On exécute 100 fois les requêtes en tant que "warm-up" sur 30% des requêtes
                .executeQuerySet(
                        500,
                        0.3F,
                        new File(QUERY_500k),
                        //(store, query) -> GenericFOQueryEvaluator.defaultInstance().evaluate(query.asFOQuery(), store)
                        ConcuRDFHexaStore::match
                        //ConcuRDFHexaStore::match
                );
        System.out.println("--- Warm-up done ---");


        // Puis on commence à mesurer:
        var measure = benchmarker.measureQuerySet(
                new File(QUERY_500k),
                //(store, query) -> GenericFOQueryEvaluator.defaultInstance().evaluate(query.asFOQuery(), store)
                ConcuRDFHexaStore::match
                //ConcuRDFHexaStore::match
        );
        System.out.println(measure);
    }

}