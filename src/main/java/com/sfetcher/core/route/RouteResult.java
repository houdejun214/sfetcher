package com.sfetcher.core.route;


import com.google.common.base.Preconditions;

import java.util.*;

public class RouteResult<T> {
    private final T                         target;
    private final Map<String, String>       pathParams;
    private final Map<String, List<String>> queryParams;

    /** The maps will be wrapped in Collections.unmodifiableMap. */
    public RouteResult(T target, Map<String, String> pathParams, Map<String, List<String>> queryParams) {
        this.target      = Preconditions.checkNotNull(target, "target");
        this.pathParams  = Collections.unmodifiableMap(Preconditions.checkNotNull(pathParams,  "pathParams"));
        this.queryParams = Collections.unmodifiableMap(Preconditions.checkNotNull(queryParams, "queryParams"));
    }

    public T target() {
        return target;
    }

    public boolean found(){
        return target!=null;
    }

    /** Returns all params embedded in the request path. */
    public Map<String, String> pathParams() {
        return pathParams;
    }

    /** Returns all params in the query part of the request URI. */
    public Map<String, List<String>> queryParams() {
        return queryParams;
    }


    /**
     * Extracts the first matching param in {@code queryParams}.
     *
     * @return {@code null} if there's no match
     */
    public String queryParam(String name) {
        List<String> values = queryParams.get(name);
        return (values == null)? null : values.get(0);
    }

    /**
     * Extracts the param in {@code pathParams} first, then falls back to the first matching
     * param in {@code queryParams}.
     *
     * @return {@code null} if there's no match
     */
    public String param(String name) {
        String pathValue = pathParams.get(name);
        return (pathValue == null)? queryParam(name) : pathValue;
    }

    /**
     * Extracts all params in {@code pathParams} and {@code queryParams} matching the name.
     *
     * @return Unmodifiable list; the list is empty if there's no match
     */
    public List<String> params(String name) {
        List<String> values = queryParams.get(name);
        String       value  = pathParams.get(name);

        if (values == null) {
            return (value == null)? Collections.<String>emptyList() : Arrays.asList(value);
        }

        if (value == null) {
            return Collections.unmodifiableList(values);
        } else {
            List<String> aggregated = new ArrayList(values.size() + 1);
            aggregated.addAll(values);
            aggregated.add(value);
            return Collections.unmodifiableList(aggregated);
        }
    }
}
