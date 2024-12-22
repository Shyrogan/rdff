package qengine.benchmark;

import com.google.common.collect.Streams;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.Query;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.*;
import java.util.*;

public class QueryDatasetProcessor {
    private static final String WORKING_DIR = "data/";
    private static final String DATASET_FILE = WORKING_DIR + "2M/jeu2M.nt";
    //private static final String DATASET_FILE = WORKING_DIR + "500k/jeu500K.nt";
    private static final String QUERYSET_DIR = WORKING_DIR + "set2/";
    private static final double EMPTY_QUERIES_RATIO = 0.05; // 5% de requêtes vides à conserver

    public static void main(String[] args) throws IOException {
        // Chargement des données RDF
        System.out.println("=== Parsing RDF Data ===");
        List<RDFAtom> rdfAtoms = parseRDFData(DATASET_FILE);
        RDFHexaStore hexaFactBase = initializeHexaStore(rdfAtoms);

        // Chargement et analyse des requêtes
        System.out.println("\n=== Loading and Analyzing Queries ===");
        List<StarQuery> allQueries = loadAllQueriesFromDirectory(QUERYSET_DIR);
        int queriesNuber = allQueries.size();
        System.out.println("\ninital queries number: " + queriesNuber);
        QueryAnalysisResult analysisResult = analyzeQueries(allQueries, hexaFactBase);

        // Génération du jeu de données final
        System.out.println("\n=== Generating Final Query Dataset ===");
        List<StarQuery> finalDataset = generateFinalDataset(analysisResult);

        // Export du jeu de données final
        System.out.println("\n=== Exporting Final Dataset ===");
        exportQueries(finalDataset, WORKING_DIR + "2M/jeu2M.nt");
        //exportQueries(finalDataset, WORKING_DIR + "500k/jeu2M.nt");

        // Affichage des statistiques
        printStatistics(analysisResult, finalDataset);
    }

    private static RDFHexaStore initializeHexaStore(List<RDFAtom> atoms) {
        RDFHexaStore store = new RDFHexaStore();
        atoms.forEach(store::add);
        return store;
    }

    private static QueryAnalysisResult analyzeQueries(List<StarQuery> queries, RDFHexaStore hexaStore) {
        Set<StarQuery> uniqueQueries = new HashSet<>();
        List<StarQuery> emptyQueries = new ArrayList<>();
        List<StarQuery> nonEmptyQueries = new ArrayList<>();

        Map<Long, Long> counts = new HashMap<>();

        for (StarQuery query : queries) {
            if (!uniqueQueries.add(query)) {
                continue; // Skip duplicate queries
            }

            Iterator<Substitution> results = hexaStore.match(query);
            long count = Streams.stream(results)
                    .count();
            if (count == 0L) {
                emptyQueries.add(query);
            } else {
                nonEmptyQueries.add(query);
            }
            counts.put(count, counts.getOrDefault(count, 0L) + 1);
        }

        return new QueryAnalysisResult(uniqueQueries, emptyQueries, nonEmptyQueries, counts);
    }

    private static List<StarQuery> generateFinalDataset(QueryAnalysisResult analysis) {
        List<StarQuery> finalDataset = new ArrayList<>();
        System.out.println("unique queries: " + analysis.uniqueQueries().size());
        // Calculer le nombre de requêtes vides à conserver
        int nonEmptyQueries = analysis.nonEmptyQueries().size();
        System.out.println("non empty queries: " + nonEmptyQueries);
        int emptyQueriesToKeep = (int) Math.ceil(nonEmptyQueries * EMPTY_QUERIES_RATIO);
        System.out.println("maximum empty queries to kept: : " + emptyQueriesToKeep);

        // Sélectionner les requêtes vides aléatoirement
        List<StarQuery> selectedEmptyQueries = selectRandomQueries(
                analysis.emptyQueries(), emptyQueriesToKeep);

        // Combiner les requêtes
        finalDataset.addAll(analysis.nonEmptyQueries());
        finalDataset.addAll(selectedEmptyQueries);

        return finalDataset;
    }

    private static List<StarQuery> selectRandomQueries(List<StarQuery> queries, int count) {
        List<StarQuery> copyList = new ArrayList<>(queries);
        Collections.shuffle(copyList);
        List<StarQuery> returnedList = copyList.subList(0, Math.min(count, copyList.size()));
        System.out.println("kept empty queries :" + returnedList.size());
        return returnedList;
    }

    private static void printStatistics(QueryAnalysisResult analysis, List<StarQuery> finalDataset) {
        System.out.println("\n=== Analysis Results ===");
        System.out.printf("unique queries: %d%n", analysis.uniqueQueries().size());
        System.out.printf("Empty queries found: %d%n", analysis.emptyQueries().size());
        System.out.printf("Non-empty queries: %d%n", analysis.nonEmptyQueries().size());
        System.out.printf("Final dataset size: %d%n", finalDataset.size());
        System.out.printf("Histogram: %s%%n", analysis.counts().toString());
    }

    // Record pour stocker les résultats de l'analyse
    private record QueryAnalysisResult(
            Set<StarQuery> uniqueQueries,
            List<StarQuery> emptyQueries,
            List<StarQuery> nonEmptyQueries,
            Map<Long, Long> counts
    ) {
    }

    ;

    /**
     * Charge toutes les requêtes de tous les fichiers d'un répertoire.
     *
     * @param directoryPath Chemin du répertoire contenant les fichiers de requêtes
     * @return Liste de toutes les StarQueries parsées
     * @throws IOException Si une erreur survient lors de la lecture des fichiers
     */
    private static List<StarQuery> loadAllQueriesFromDirectory(String directoryPath) throws IOException {
        File directory = new File(directoryPath);
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Le chemin spécifié n'est pas un répertoire : " + directoryPath);
        }

        List<StarQuery> allQueries = new ArrayList<>();
        File[] queryFiles = directory.listFiles((dir, name) -> name.endsWith(".queryset"));
        if (queryFiles == null) {
            throw new IOException("Impossible de lire les fichiers dans le répertoire : " + directoryPath);
        }

        for (File queryFile : queryFiles) {
            System.out.println("Parsing queries from file: " + queryFile.getName());
            allQueries.addAll(parseSparQLQueries(queryFile.getPath()));
        }
        return allQueries;
    }

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

    /**
     * Exporte les requêtes uniques dans un fichier.
     *
     * @param queries  Set des requêtes uniques
     * @param filename Le nom du fichier de sortie
     * @throws IOException Si une erreur survient lors de l'écriture du fichier
     */
    private static void exportQueries(List<StarQuery> queries, String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (StarQuery query : queries) {
                writer.write(query.getLabel());
                writer.newLine();
            }
        }
    }

}
