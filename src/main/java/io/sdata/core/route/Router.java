package io.sdata.core.route;


import com.lakeside.core.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Router that match the url path with specify handler
 * matching orders.
 */
final public class Router<T> {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    // A path can only point to one target
    private final Map<Path, T> routes = new HashMap<Path, T>();

    /** Returns all routes in this router, an unmodifiable map of {@code Path -> Target}. */
    public Map<Path, T> routes() {
        return Collections.unmodifiableMap(routes);
    }

    /**
     * This method does nothing if the path has already been added.
     * A path can only point to one target.
     */
    public Router<T> addRoute(String path, T target) {
        Path p = new Path(path);
        if (routes.containsKey(p)) {
            return this;
        }
        routes.put(p, target);
        return this;
    }

    //--------------------------------------------------------------------------

    /** Removes the route specified by the path. */
    public void removePath(String path) {
        Path p = new Path(path);
        T  target = routes.remove(p);
        if (target == null) {
            return;
        }
    }

    /** @return {@code null} if no match; note: {@code queryParams} is not set in {@link RouteResult} */
    public RouteResult<T> route(String requestPath) {
        if (StringUtils.isEmpty(requestPath)) {
            return null;
        }
        // Optimization note:
        // - Reuse tokens and pathParams in the loop
        // - decoder doesn't decode anything if decoder.parameters is not called
        Map<String, String> pathParams = new HashMap<>();
        for (Map.Entry<Path, T> entry : routes.entrySet()) {
            Path path = entry.getKey();
            if (path.match(requestPath, pathParams)) {
                T target = entry.getValue();
                return new RouteResult(target, pathParams, Collections.emptyMap());
            }
            // Reset for the next loop
            pathParams.clear();
        }
        return null;
    }
}
