package qengine.benchmark;

import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.Query;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QueryEngineBenchmark {
    private static final String WORKING_DIR = "data/";
    private static final String DATASET_FILE = WORKING_DIR + "2M/jeu2M.nt";
    private static final String QUERYSET_FILE = WORKING_DIR + "2M/queries.queryset";

    private static final String OUTPUT_FILE = "benchmark_results.csv";
    private static final double WARMUP_RATIO = 0.3; // 30% pour le warmup

    public static void main(String[] args) throws IOException {

        System.out.println("=== Parsing RDF Data ===");
        List<RDFAtom> rdfAtoms = parseRDFData(DATASET_FILE);
        RDFHexaStore hexaStore = initializeHexaStore(rdfAtoms);


        System.out.println("\n=== Parsing Sample Queries ===");
        List<StarQuery> starQueries = parseSparQLQueries(QUERYSET_FILE);

        Collections.shuffle(starQueries); // Mélange aléatoire

        // Séparation warmup/benchmark
        BenchmarkSets benchmarkSets = splitQueries(starQueries);

        // Exécution du warmup
        System.out.println("=== Executing Warmup Queries ===");
        executeWarmup(benchmarkSets.warmupQueries, hexaStore);

        // Exécution du benchmark
        System.out.println("\n=== Executing Benchmark Queries ===");
        List<QueryExecutionResult> results = executeBenchmark(
                benchmarkSets.benchmarkQueries, hexaStore);

        // Export des résultats
        exportResults(results, OUTPUT_FILE);
    }

    private static RDFHexaStore initializeHexaStore(List<RDFAtom> atoms) {
        RDFHexaStore store = new RDFHexaStore();
        atoms.forEach(store::add);
        return store;
    }


    private static BenchmarkSets splitQueries(List<StarQuery> queries) {
        int warmupSize = (int) (queries.size() * WARMUP_RATIO);
        return new BenchmarkSets(
                queries.subList(0, warmupSize),
                queries.subList(warmupSize, queries.size())
        );
    }

    private static void executeWarmup(List<StarQuery> warmupQueries, RDFHexaStore hexaStore) {
        int count = 0;
        for (StarQuery query : warmupQueries) {
            executeQuery(query, hexaStore);
            count++;
            if (count % 100 == 0) {
                System.out.printf("Executed %d warmup queries%n", count);
            }
        }
    }

    private static List<QueryExecutionResult> executeBenchmark(
            List<StarQuery> queries, RDFHexaStore hexaStore) {
        List<QueryExecutionResult> results = new ArrayList<>();

        for (StarQuery query : queries) {
            long startTime = System.nanoTime();
            int resultCount = executeQuery(query, hexaStore);
            long executionTime = System.nanoTime() - startTime;

            results.add(new QueryExecutionResult(
                    query.getLabel(),
                    resultCount,
                    executionTime
            ));
        }

        return results;
    }

    private static int executeQuery(StarQuery query, RDFHexaStore hexaStore) {
        Iterator<Substitution> results = hexaStore.match(query);
        int count = 0;
        while (results.hasNext()) {
            results.next();
            count++;
        }
        return count;
    }

    private static void exportResults(List<QueryExecutionResult> results, String outputFile)
            throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("resultCount,executionTimeNs\n");
            for (QueryExecutionResult result : results) {
                writer.write(String.format("%d,%d%n",
                        result.resultCount(),
                        result.executionTimeNs()
                ));
            }
        }
    }

    // Classes de données
    private record BenchmarkSets(
            List<StarQuery> warmupQueries,
            List<StarQuery> benchmarkQueries
    ) {}

    private record QueryExecutionResult(
            String queryLabel,
            int resultCount,
            long executionTimeNs
    ) {}


    /**
     * Parse et affiche le contenu d'un fichier RDF.
     *
     * @param rdfFilePath Chemin vers le fichier RDF à parser
     * @return Liste des RDFAtoms parsés
     */
    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            while (rdfAtomParser.hasNext()) {
                rdfAtoms.add(rdfAtomParser.next());
            }
        }
        return rdfAtoms;
    }

    /**
     * Parse et affiche le contenu d'un fichier de requêtes SparQL.
     *
     * @param queryFilePath Chemin vers le fichier de requêtes SparQL
     * @return Liste des StarQueries parsées
     */
    private static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
        List<StarQuery> starQueries = new ArrayList<>();

        try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery starQuery) {
                    starQueries.add(starQuery);
                }
            }
        }
        return starQueries;
    }
}