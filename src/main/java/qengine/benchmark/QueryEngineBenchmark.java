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





    public static void main(String[] args) throws IOException {
        // Beaucoup de codes qui se répètent, peut-être faire une abstraction un peu dans le style de ce que j'ai tenté de cook avec



        var benchmarker = new Benchmarker<>(new SimpleInMemoryGraphStore())  // RDFHexaStore() |  ConcuRDFHexaStore()
                // On charge les données et on les ajoute au store
                .loadDataSet(new File( DATASET_500K), SimpleInMemoryGraphStore::add)

                // On exécute 100 fois les requêtes en tant que "warm-up"
                .executeQuerySet(100, new File(QUERY_500k),  (store, query) ->
                        GenericFOQueryEvaluator.defaultInstance()           //    store::match
                                .evaluate(query.asFOQuery(), store)
                );
                System.out.println("\n Wamup done \n");


        // Puis on commence à mesurer:
        var measure = benchmarker.measureQuerySet(new File(QUERY_500k),  (store, query) ->
                GenericFOQueryEvaluator.defaultInstance()
                        .evaluate(query.asFOQuery(), store)                 //    store::match
        );
        System.out.println(measure);
    }

}