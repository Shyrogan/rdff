package qengine.program;

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

public final class QuerySetEvaluator {

    private static final String WORKING_DIR = "data/";
    private static final String DATASET_FILE = WORKING_DIR + "benchmark_data.nt";
    private static final String QUERYSET_DIR = WORKING_DIR + "queryset/";
    private static final String QUERYSET = WORKING_DIR + "unique_queries.queryset";


    public static void main(String[] args) throws IOException {
        System.out.println("=== Parsing RDF Data ===");
        List<RDFAtom> rdfAtoms = parseRDFData(DATASET_FILE);

        System.out.println("\n=== Loading Queries from All Query Files ===");
        List<StarQuery> starQueries = loadAllQueriesFromDirectory(QUERYSET_DIR);
        //List<StarQuery> starQueries = parseSparQLQueries(QUERYSET);


        System.out.println("\n=== Initializing RDFHexaStore ===");
        RDFHexaStore hexaFactBase = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            hexaFactBase.add(atom);
        }

        System.out.println("\n=== Shuffling Queries ===");
        Collections.shuffle(starQueries); // Mélanger les requêtes

        int warmupSize = (int) (starQueries.size() * 0.3); // 30% pour le warm-up
        List<StarQuery> warmupQueries = starQueries.subList(0, warmupSize);
        //List<StarQuery> evaluationQueries = starQueries.subList(warmupSize, starQueries.size());
        List<StarQuery> evaluationQueries = starQueries.subList(0, starQueries.size());

        System.out.println("\n=== Warming Up System ===");
        /*for (StarQuery query : warmupQueries) {
            executeStarQuery(query, hexaFactBase, null); // Exécution sans enregistrement
        }*/

        System.out.println("\n=== Evaluating Remaining Queries ===");
        Set<StarQuery> uniqueQueries = new HashSet<>(); // Set pour stocker les requêtes uniques
        int duplicateCount = 0;
        Map<StarQuery, Integer> queryCounts = new HashMap<>();
        Map<Integer, Integer> cardinalityDistribution = new HashMap<>();

        for (StarQuery query : evaluationQueries) {
            List<String> rawResults = new ArrayList<>();
            Set<String> uniqueResults = new HashSet<>();

            Iterator<Substitution> queryResults = hexaFactBase.match(query);
            while (queryResults.hasNext()) {
                String result = queryResults.next().toString();
                rawResults.add(result);
                uniqueResults.add(result);
            }

            int totalResults = rawResults.size();
            int uniqueCount = uniqueResults.size();

            if (!uniqueQueries.add(query)) {
                duplicateCount++; //
            }

            // Mettre à jour la distribution de cardinalité
            cardinalityDistribution.put(totalResults,
                    cardinalityDistribution.getOrDefault(totalResults, 0) + 1);

            queryCounts.put(query, totalResults);

            if (uniqueCount < totalResults) {
                System.out.printf("Query '%s' has %d duplicates%n", query, totalResults - uniqueCount);
            }
        }

        // Afficher les doublons
        System.out.println("\n=== Duplicate Queries ===");
        System.out.printf("Total duplicate queries: %d%n", duplicateCount);

        // Exporter les requêtes uniques dans un fichier
        System.out.println("\n=== Exporting Unique Queries ===");
        exportUniqueQueries(uniqueQueries, "unique_queries.queryset");

        System.out.println("\n=== Results Summary ===");
        System.out.printf("Total queries evaluated: %d%n", evaluationQueries.size());
        long emptyQueries = queryCounts.values().stream().filter(count -> count == 0).count();
        System.out.printf("Empty queries: %d%n", emptyQueries);

        System.out.println("\n=== Cardinality Distribution ===");
        cardinalityDistribution.forEach((cardinality, queryCount) -> {
            System.out.printf("Queries with %d results: %d%n", cardinality, queryCount);
        });
    }

    /**
     * Exporte les requêtes uniques dans un fichier.
     *
     * @param uniqueQueries Set des requêtes uniques
     * @param filename      Le nom du fichier de sortie
     * @throws IOException Si une erreur survient lors de l'écriture du fichier
     */
    private static void exportUniqueQueries(Set<StarQuery> uniqueQueries, String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (StarQuery query : uniqueQueries) {
                writer.write(query.getLabel());
                writer.newLine();
            }
        }
    }


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
     * Exécute une requête en étoile sur le RDFHexaStore et met à jour le compteur de résultats.
     *
     * @param starQuery    La requête à exécuter
     * @param hexaFactBase Le store contenant les atomes
     * @param resultCounts Map pour compter les résultats (peut être null si inutile)
     * @return Ensemble des résultats pour la requête
     */
    private static Set<String> executeStarQuery(StarQuery starQuery, RDFHexaStore hexaFactBase,
                                                Map<StarQuery, Integer> resultCounts) {
        Set<String> results = new HashSet<>();
        Iterator<Substitution> queryResults = hexaFactBase.match(starQuery);

        while (queryResults.hasNext()) {
            Substitution result = queryResults.next();
            results.add(result.toString());
        }

        if (resultCounts != null) {
            resultCounts.put(starQuery, results.size());
        }

        return results;
    }
}
