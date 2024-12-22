package qengine.benchmark;

import qengine.storage.RDFHexaStore;

import java.io.*;

public class QueryEngineBenchmark {


    public static void main(String[] args) throws IOException {
        // Beaucoup de codes qui se répètent, peut-être faire une abstraction un peu dans le style de ce que j'ai tenté de cook avec
        // le benchmark
        var benchmarker = new Benchmarker<>(new RDFHexaStore())
                // On charge les données et on les ajoute au store
                .loadDataSet(new File( "data/2M/jeu2M.nt"), RDFHexaStore::add)
                // On exécute 100 fois les requêtes en tant que "warm-up"
                .executeQuerySet(100, new File("data/2M/queries.queryset"), RDFHexaStore::match);

        // Puis on commence à mesurer:
        var measure = benchmarker.measureQuerySet(new File("data/2M/queries.queryset"), RDFHexaStore::match);
        System.out.println(measure);
    }

}