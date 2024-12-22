package concurentStore;

import fr.boreal.model.logicalElements.api.Term;

import java.util.HashMap;
import java.util.Map;

public class ConcuRDFDictionary {
    private final Map<Term, Integer> resourceToId = new HashMap<>();
    private final Map<Integer, Term> idToResource = new HashMap<>();
    private int nextId = 1;

    public int encode(Term resource) {
        return resourceToId.computeIfAbsent(resource, r -> {
            int id = nextId++;
            idToResource.put(id, r);
            return id;
        });
    }

    public Term decode(int id) {
        return idToResource.get(id);
    }

}