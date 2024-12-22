package qengine.benchmark;

import fr.boreal.model.query.api.Query;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;

public class Benchmarker<S> {

    private final S store;

    public Benchmarker(S store) {
        this.store = store;
    }

    public Benchmarker<S> loadDataSet(File file, BiConsumer<S, RDFAtom> function) {
        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(file)) {
            while (rdfAtomParser.hasNext()) {
                function.accept(store, rdfAtomParser.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    public Benchmarker<S> executeQuerySet(int factor, File file, BiConsumer<S, StarQuery> function) {
        final var queries = new LinkedList<StarQuery>();
        try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(file.getPath())) {
            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery starQuery) {
                    queries.add(starQuery);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < factor; i++) {
            System.out.println("Factor " + i + ": " + queries.size());
            Collections.shuffle(queries);
            for (var query : queries) {
                function.accept(store, query);
            }
        }

        return this;
    }

    public Measurement measureQuerySet(File file, BiConsumer<S, StarQuery> function) {
        long totalNanoTime;
        int queryCount = 0;

        try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(file.getPath())) {
            List<StarQuery> starQueries = new LinkedList<>();

            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery starQuery) {
                    starQueries.add(starQuery);
                    queryCount++;
                }
            }
            // Measure execution of all queries as a group
            long nanoStart = System.nanoTime();
            for (StarQuery starQuery : starQueries) {
                function.accept(store, starQuery);
            }
            totalNanoTime = System.nanoTime() - nanoStart;
        } catch (IOException e) {
            e.printStackTrace();
            return new Measurement(0.0, 0.0); // Return zero if an exception occurs
        }

        return new Measurement(
                totalNanoTime / 1.0e6, // Convert nanoseconds to milliseconds
                queryCount > 0 ? (totalNanoTime / (double) queryCount) / 1.0e6 : 0.0
        );
    }

    public record Measurement(double totalTime, double avgRequestTime) {}

}

