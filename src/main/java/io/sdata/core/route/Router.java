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

    // Reverse index to create reverse routes fast (a target can have multiple paths)
    private final Map<T, Set<Path>> reverseRoutes = new HashMap<T, Set<Path>>();


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
        if (routes.containsKey(path)) {
            return this;
        }

        routes.put(p, target);
        addReverseRoute(target, p);
        return this;
    }

    private void addReverseRoute(T target, Path path) {
        Set<Path> paths = reverseRoutes.get(target);
        if (paths == null) {
            paths = new HashSet<Path>();
            paths.add(path);
            reverseRoutes.put(target, paths);
        } else {
            paths.add(path);
        }
    }

    //--------------------------------------------------------------------------

    /** Removes the route specified by the path. */
    public void removePath(String path) {
        Path p = new Path(path);
        T  target = routes.remove(p);
        if (target == null) {
            return;
        }

        Set<Path> paths = reverseRoutes.remove(target);
        paths.remove(p);
    }


    //--------------------------------------------------------------------------

    /** @return {@code null} if no match; note: {@code queryParams} is not set in {@link RouteResult} */
    public RouteResult<T> route(String path) {
        return route(StringUtils.split(path, "/"));
    }

    /** @return {@code null} if no match; note: {@code queryParams} is not set in {@link RouteResult} */
    private RouteResult<T> route(String[] requestPathTokens) {
        // Optimization note:
        // - Reuse tokens and pathParams in the loop
        // - decoder doesn't decode anything if decoder.parameters is not called
        Map<String, String> pathParams = new HashMap<String, String>();
        for (Map.Entry<Path, T> entry : routes.entrySet()) {
            Path path = entry.getKey();
            if (path.match(requestPathTokens, pathParams)) {
                T target = entry.getValue();
                return new RouteResult(target, pathParams, Collections.emptyMap());
            }

            // Reset for the next loop
            pathParams.clear();
        }

        return null;
    }
}