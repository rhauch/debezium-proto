/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Patch;
import org.debezium.driver.Debezium;
import org.debezium.driver.Schema;
import org.debezium.driver.SessionToken;
import org.debezium.server.annotation.PATCH;

/**
 * @author Randall Hauch
 *
 */
@Path("/db")
public final class Databases {
    
    private static final CacheControl NO_CACHE = CacheControl.valueOf("no-cache");

    private final Debezium driver;
    private final long defaultTimeout;
    private final TimeUnit timeoutUnit;

    public Databases( Debezium driver, long defaultTimeout, TimeUnit timeoutUnit ) {
        this.driver = driver;
        this.defaultTimeout = defaultTimeout;
        this.timeoutUnit = timeoutUnit;
    }
    
    // -----------------------------------------------------------------------------------------------
    // Schema methods
    // -----------------------------------------------------------------------------------------------

    /**
     * Get the schemas for all of the databases for which the user has access.
     * @param headers the HTTP headers in the request
     * @param uri the request URI
     * @return the schemas
     */
    @GET
    @Produces("text/plain")
    public String getSchemas(@Context HttpHeaders headers,
                             @Context UriInfo uri) {
        SessionToken token = validateSession(headers, uri);
        return "hello world";
    }

    @GET
    @Path("{dbid}")
    @Produces("text/plain")
    public StreamingOutput getSchema(@Context HttpHeaders headers,
                                     @Context UriInfo uri,
                                     @PathParam("id") int id) {
        SessionToken token = validateSession(headers, uri);
        // throw new WebApplicationException(Response.Status.NOT_FOUND);
        return output -> {
            PrintStream writer = new PrintStream(output);
            writer.print("Hello, World");
        };
    }

    // -----------------------------------------------------------------------------------------------
    // Entity Type methods
    // -----------------------------------------------------------------------------------------------

    @GET
    @Path("{dbid}/t")
    @Produces("text/plain")
    public Response getEntityTypes(@Context HttpHeaders headers,
                                   @Context UriInfo uri,
                                   @PathParam("dbid") String dbId) {
        SessionToken token = validateSession(headers, uri);
        Schema schema = driver.readSchema(token, dbId, defaultTimeout, timeoutUnit);
        Document result = Document.create();
        schema.entityTypes().forEach(result::set);
        return Response.ok()
                       .entity(result)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @GET
    @Path("{dbid}/t/{type}")
    @Produces("text/plain")
    public Response getEntityType(@Context HttpHeaders headers,
                                  @Context UriInfo uri,
                                  @PathParam("dbid") String dbId,
                                  @PathParam("type") String entityType) {
        SessionToken token = validateSession(headers, uri);
        EntityType type = Identifier.of(dbId, entityType);
        return Response.ok()
                       .entity("get entity type " + type)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @PUT
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes("application/json")
    @Produces("text/plain")
    public Response updateEntityType(@Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 Document entity) {
        SessionToken token = validateSession(headers, uri);
        EntityType type = Identifier.of(dbId, entityType);
        return Response.ok()
                       .entity("updated entity type " + type)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @DELETE
    @Path("{dbid}/t/{type}/e/{ent}")
    @Produces("text/plain")
    public Response deleteEntityType(@Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType) {
        SessionToken token = validateSession(headers, uri);
        EntityType type = Identifier.of(dbId, entityType);
        return Response.ok()
                       .entity("deleted entity type " + type)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @PATCH
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes("application/json-patch+json")
    @Produces("text/plain")
    public Response patchEntityType(@Context HttpHeaders headers,
                                @Context UriInfo uri,
                                @PathParam("dbid") String dbId,
                                @PathParam("type") String entityType,
                                @PathParam("ent") String entityId,
                                Array operations) {
        SessionToken token = validateSession(headers, uri);
        EntityType type = Identifier.of(dbId, entityType);
        Patch<EntityType> patch = Patch.from(type, operations);
        // Submit the patch ...

        // Complete the response ...
        return Response.ok()
                       .entity("patching entity type " + type)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    // -----------------------------------------------------------------------------------------------
    // Entity methods
    // -----------------------------------------------------------------------------------------------

    @POST
    @Path("{dbid}/t/{type}/e")
    @Consumes("application/json")
    @Produces("text/plain")
    public Response createEntity(@Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 Document entity) {
        SessionToken token = validateSession(headers, uri);
        EntityId id = Identifier.newEntity(dbId, entityType);
        return Response.ok()
                       .entity("created entity " + id)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @GET
    @Path("{dbid}/t/{type}/e/{ent}")
    @Produces("text/plain")
    public Response getEntityById(@Context HttpHeaders headers,
                                  @Context UriInfo uri,
                                  @PathParam("dbid") String dbId,
                                  @PathParam("type") String entityType,
                                  @PathParam("ent") String entityId) {
        SessionToken token = validateSession(headers, uri);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        return Response.ok()
                       .entity("hello, entity " + id.id() + " of type '" + id.type().entityTypeName() + "' in database " + id.databaseId() + "!")
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @PUT
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes("application/json")
    @Produces("text/plain")
    public Response updateEntity(@Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 @PathParam("ent") String entityId,
                                 Document entity) {
        SessionToken token = validateSession(headers, uri);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Patch<EntityId> patch = Patch.replace(id,entity);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch,headers);
        
        // Submit the patch to destroy the entity (perhaps conditionally) ...
        
        // Complete the response ...
        return Response.ok()
                       .entity("updated entity " + id)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @DELETE
    @Path("{dbid}/t/{type}/e/{ent}")
    @Produces("text/plain")
    public Response deleteEntity(@Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 @PathParam("ent") String entityId) {
        SessionToken token = validateSession(headers, uri);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Patch<EntityId> patch = Patch.destroy(id);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch,headers);
        
        // Submit the patch to destroy the entity (perhaps conditionally) ...
        
        // Complete the response ...
        return Response.ok()
                       .entity("deleted entity " + id)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }

    @PATCH
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes("application/json-patch+json")
    @Produces("text/plain")
    public Response patchEntity(@Context HttpHeaders headers,
                                @Context UriInfo uri,
                                @PathParam("dbid") String dbId,
                                @PathParam("type") String entityType,
                                @PathParam("ent") String entityId,
                                Array operations) {
        SessionToken token = validateSession(headers, uri);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Patch<EntityId> patch = Patch.from(id, operations);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch,headers);

        // Submit the patch ...

        // Complete the response ...
        return Response.ok()
                       .entity("patching entity " + id)
                       .lastModified(null)
                       .cacheControl(NO_CACHE)
                       .link(uri.getAbsolutePath(), "remove")
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .tag("etag")
                       .build();
    }
    
    private <IdType extends Identifier> Patch<IdType> addConstraints( Patch<IdType> patch, HttpHeaders headers ) {
        String matchEtag = headers.getHeaderString("If-Match");
        if ( matchEtag != null ) {
            // Add an operation that tests the special "$etag" field in entities ...
            patch.edit().require("/$etag", Value.create(matchEtag)).end();
        }
        return patch;
    }
    
    private SessionToken validateSession(HttpHeaders headers, UriInfo uri) {
        // The application's API key is used here ...
        String apiKey = uri.getQueryParameters().getFirst("key");

        // First try the authorization header, of the form "Authorization: Bearer <oathToken>" ...
        List<String> authHeaders = headers.getRequestHeaders().get("Authorization");
        if (authHeaders != null && authHeaders.size() >= 2 && "Bearer".equals(authHeaders.get(0))) {
            return validateSession(authHeaders.get(1), apiKey);
        }
        // Next try the first "access_token" query parameter ...
        String accessToken = uri.getQueryParameters().getFirst("access_token");
        if (accessToken != null && !accessToken.isEmpty()) {
            return validateSession(accessToken, apiKey);
        }
        // Next try a cookie
        Cookie cookie = headers.getCookies().get("access_token");
        if (cookie != null) {
            return validateSession(cookie.getValue(), apiKey);
        }
        // Not previously authorized ...
        return null;
    }

    /**
     * Validate the user's existing session.
     * 
     * @param token the string representation of the token; should not be null
     * @param apiKey the calling application's API key
     * @return the existing session token, or null if the user is not current authenticated
     */
    private SessionToken validateSession(String token, String apiKey) {
        return null;
    }

}
