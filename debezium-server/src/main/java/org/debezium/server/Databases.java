/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.annotation.security.RolesAllowed;
import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.debezium.driver.DebeziumDriver;
import org.debezium.driver.SessionToken;
import org.debezium.message.Array;
import org.debezium.message.Document;
import org.debezium.message.Message.Field;
import org.debezium.message.Patch;
import org.debezium.message.Value;
import org.debezium.model.Entity;
import org.debezium.model.EntityChange;
import org.debezium.model.EntityCollection;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.EntityTypeChange;
import org.debezium.model.Identifier;
import org.debezium.model.Schema;
import org.debezium.server.annotation.PATCH;
import org.keycloak.KeycloakSecurityContext;
import org.keycloak.representations.IDToken;

/**
 * @author Randall Hauch
 *
 */
@Path("/db")
public final class Databases {

    private static final CacheControl NO_CACHE = CacheControl.valueOf("no-cache");

    private final DebeziumDriver driver;
    private final long defaultTimeout;
    private final TimeUnit timeoutUnit;

    public Databases(DebeziumDriver driver, long defaultTimeout, TimeUnit timeoutUnit) {
        this.driver = driver;
        this.defaultTimeout = defaultTimeout;
        this.timeoutUnit = timeoutUnit;
    }

    // -----------------------------------------------------------------------------------------------
    // Schema methods
    // -----------------------------------------------------------------------------------------------

    @GET
    @Path("{dbid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSchema(@Context HttpServletRequest request,
                              @Context UriInfo uri,
                              @PathParam("dbid") String dbId) {
        SessionToken token = validateSession(request, dbId);
        Schema schema = driver.readSchema(token, dbId, defaultTimeout, timeoutUnit);
        return Response.ok()
                       .entity(schema.asDocument())
                       .lastModified(timestamp(schema.lastModified()))
                       .tag(Long.toString(schema.revision()))
                       .cacheControl(NO_CACHE)
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    // -----------------------------------------------------------------------------------------------
    // Entity Type methods
    // -----------------------------------------------------------------------------------------------

    @GET
    @Path("{dbid}/t")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getEntityTypes(@Context HttpServletRequest request,
                                   @Context UriInfo uri,
                                   @PathParam("dbid") String dbId) {
        SessionToken token = validateSession(request, dbId);
        Schema schema = driver.readSchema(token, dbId, defaultTimeout, timeoutUnit);
        Document result = Document.create();
        schema.entityTypes().forEach(result::set);
        return Response.ok()
                       .entity(result)
                       .lastModified(timestamp(schema.lastModified()))
                       .tag(Long.toString(schema.revision()))
                       .cacheControl(NO_CACHE)
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    @GET
    @Path("{dbid}/t/{type}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getEntityType(@Context HttpServletRequest request,
                                  @Context UriInfo uri,
                                  @PathParam("dbid") String dbId,
                                  @PathParam("type") String entityType) {
        SessionToken token = validateSession(request, dbId);
        Schema schema = driver.readSchema(token, dbId, defaultTimeout, timeoutUnit);
        EntityCollection collection = schema.entityTypes().get(entityType);
        if (collection != null) {
            Document result = collection.document();
            return Response.ok()
                           .entity(result)
                           .lastModified(timestamp(collection.lastModified()))
                           .tag(Long.toString(collection.revision()))
                           .cacheControl(NO_CACHE)
                           .type(MediaType.APPLICATION_JSON_TYPE)
                           .link(uri.getAbsolutePath(), "remove")
                           .build();
        }
        return Response.status(Status.NOT_FOUND)
                       .entity(Document.create("message", "The entity type '" + entityType + "' does not exist"))
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    @PUT
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @RolesAllowed("developer")
    public Response updateEntityType(@Context HttpServletRequest request,
                                     @Context HttpHeaders headers,
                                     @Context UriInfo uri,
                                     @PathParam("dbid") String dbId,
                                     @PathParam("type") String entityTypeName,
                                     Document entityType) {
        SessionToken token = validateSession(request, dbId);
        EntityType type = Identifier.of(dbId, entityTypeName);
        Patch<EntityType> patch = Patch.replace(type, entityType);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch, headers);

        // Submit the patch to update the entity (perhaps conditionally) ...
        EntityTypeChange change = driver.changeEntityType(token, patch, defaultTimeout, timeoutUnit);

        // Complete the response ...
        switch (change.status()) {
            case OK:
                return Response.ok()
                               .entity(change.entityType().document())
                               .lastModified(timestamp(change.entityType().lastModified()))
                               .cacheControl(NO_CACHE)
                               .link(uri.getAbsolutePath(), "self")
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .tag(Long.toString(change.entityType().revision()))
                               .build();
            case DOES_NOT_EXIST:
                String msg = "The entity type '" + entityTypeName + "' does not exist in database '" + dbId + "'";
                return Response.status(Status.NOT_FOUND)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case PATCH_FAILED:
                msg = "Unable to update entity type '" + entityTypeName + "' in database '" + dbId + "'";
                return Response.status(Status.CONFLICT)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
        }
        return unexpected();
    }

    @DELETE
    @Path("{dbid}/t/{type}/e/{ent}")
    @Produces(MediaType.TEXT_PLAIN)
    @RolesAllowed("developer")
    public Response deleteEntityType(@Context HttpServletRequest request,
                                     @Context HttpHeaders headers,
                                     @Context UriInfo uri,
                                     @PathParam("dbid") String dbId,
                                     @PathParam("type") String entityTypeName) {
        SessionToken token = validateSession(request, dbId);
        EntityType type = Identifier.of(dbId, entityTypeName);
        Patch<EntityType> patch = Patch.destroy(type);
        // Check whether the request constrains the entity type that will be edited ...
        patch = addConstraints(patch, headers);

        // Submit the patch to update the entity type (perhaps conditionally) ...
        EntityTypeChange change = driver.changeEntityType(token, patch, defaultTimeout, timeoutUnit);

        // Complete the response ...
        switch (change.status()) {
            case OK:
                String msg = "Successfully removed entity type '" + entityTypeName + "' from database '" + dbId + "'";
                return Response.ok()
                               .entity(msg)
                               .cacheControl(NO_CACHE)
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case DOES_NOT_EXIST:
                msg = "The entity type '" + entityTypeName + "' does not exist in database '" + dbId + "'";
                return Response.ok()
                               .entity(msg)
                               .cacheControl(NO_CACHE)
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case PATCH_FAILED:
                msg = "Unable to delete entity type '" + entityTypeName + "' in database '" + dbId + "'";
                return Response.status(Status.CONFLICT)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
        }
        return unexpected();
    }

    @PATCH
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes("application/json-patch+json")
    @Produces(MediaType.TEXT_PLAIN)
    @RolesAllowed("developer")
    public Response patchEntityType(@Context HttpServletRequest request,
                                    @Context HttpHeaders headers,
                                    @Context UriInfo uri,
                                    @PathParam("dbid") String dbId,
                                    @PathParam("type") String entityType,
                                    @PathParam("ent") String entityId,
                                    Array operations) {
        SessionToken token = validateSession(request, dbId);
        EntityType type = Identifier.of(dbId, entityType);
        Patch<EntityType> patch = Patch.from(type, operations);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch, headers);

        // Submit the patch ...
        EntityTypeChange change = driver.changeEntityType(token, patch, defaultTimeout, timeoutUnit);

        // Complete the response ...
        switch (change.status()) {
            case OK:
                return Response.ok()
                               .entity(change.entityType().document())
                               .lastModified(timestamp(change.entityType().lastModified()))
                               .cacheControl(NO_CACHE)
                               .link(uri.getAbsolutePath(), "remove")
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .tag(Long.toString(change.entityType().revision()))
                               .build();
            case DOES_NOT_EXIST:
                String msg = "The entity type '" + entityType + "' does not exist in database '" + dbId + "'";
                return Response.status(Status.NOT_FOUND)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case PATCH_FAILED:
                msg = "Unable to patch entity type '" + entityType + "' in database '" + dbId + "'";
                return Response.status(Status.CONFLICT)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
        }
        return unexpected();
    }

    // -----------------------------------------------------------------------------------------------
    // Entity methods
    // -----------------------------------------------------------------------------------------------

    @POST
    @Path("{dbid}/t/{type}/e")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response createEntity(@Context HttpServletRequest request,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 Document entity) {
        SessionToken token = validateSession(request, dbId);
        EntityType type = Identifier.of(dbId, entityType);
        EntityChange change = driver.batch().createEntity(type)
                                    .add("/", Value.create(entity))
                                    .end()
                                    .submit(token, defaultTimeout, timeoutUnit)
                                    .changeStream().findFirst().get();
        if (change.succeeded()) {
            return Response.ok()
                           .entity(change.entity().asDocument())
                           .lastModified(null)
                           .cacheControl(NO_CACHE)
                           .link(uri.getAbsolutePath(), "remove")
                           .type(MediaType.APPLICATION_JSON_TYPE)
                           .tag("etag")
                           .build();
        }
        String msg = "Unable to create a new entity of type '" + entityType + "' in database '" + dbId + "'";
        return Response.status(Status.BAD_REQUEST)
                       .entity(error(msg, change.failureReasons()))
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    @GET
    @Path("{dbid}/t/{type}/e/{ent}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getEntityById(@Context HttpServletRequest request,
                                  @Context UriInfo uri,
                                  @PathParam("dbid") String dbId,
                                  @PathParam("type") String entityType,
                                  @PathParam("ent") String entityId) {
        SessionToken token = validateSession(request, dbId);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Entity result = driver.readEntity(token, id, defaultTimeout, timeoutUnit);
        if (result.exists()) {
            return Response.ok()
                           .entity(result.asDocument())
                           .lastModified(timestamp(result.lastModified()))
                           .cacheControl(NO_CACHE)
                           .link(uri.getAbsolutePath(), "self")
                           .type(MediaType.APPLICATION_JSON_TYPE)
                           .tag(Long.toString(result.revision()))
                           .build();
        }
        String msg = "Unable to find entity '" + entityId + " of type '" + entityType + "' in database '" + dbId + "'";
        return Response.status(Status.NOT_FOUND)
                       .entity(error(msg))
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    @PUT
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response updateEntity(@Context HttpServletRequest request,
                                 @Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 @PathParam("ent") String entityId,
                                 Document entity) {
        SessionToken token = validateSession(request, dbId);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Patch<EntityId> patch = Patch.replace(id, entity);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch, headers);

        // Submit the patch to update the entity (perhaps conditionally) ...
        EntityChange change = driver.changeEntity(token, patch, defaultTimeout, timeoutUnit);

        // Complete the response ...
        switch (change.status()) {
            case OK:
                return Response.ok()
                               .entity(change.entity().asDocument())
                               .lastModified(timestamp(change.entity().lastModified()))
                               .cacheControl(NO_CACHE)
                               .link(uri.getAbsolutePath(), "remove")
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .tag(Long.toString(change.entity().revision()))
                               .build();
            case DOES_NOT_EXIST:
                String msg = "The entity '" + entityId + "' of type '" + entityType + "' does not exist in database '" + dbId + "'";
                return Response.status(Status.NOT_FOUND)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case PATCH_FAILED:
                msg = "Unable to update entity '" + entityId + "' of type '" + entityType + "' in database '" + dbId + "'";
                return Response.status(Status.CONFLICT)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
        }
        return unexpected();
    }

    @DELETE
    @Path("{dbid}/t/{type}/e/{ent}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response deleteEntity(@Context HttpServletRequest request,
                                 @Context HttpHeaders headers,
                                 @Context UriInfo uri,
                                 @PathParam("dbid") String dbId,
                                 @PathParam("type") String entityType,
                                 @PathParam("ent") String entityId) {
        SessionToken token = validateSession(request, dbId);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Patch<EntityId> patch = Patch.destroy(id);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch, headers);

        // Submit the patch to destroy the entity (perhaps conditionally) ...
        EntityChange change = driver.changeEntity(token, patch, defaultTimeout, timeoutUnit);

        // Complete the response ...
        switch (change.status()) {
            case OK:
                String msg = "Successfully removed entity '" + entityId + "' of type '" + entityType + "' from database '" + dbId + "'";
                return Response.ok()
                               .entity(msg)
                               .cacheControl(NO_CACHE)
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case DOES_NOT_EXIST:
                msg = "The entity '" + entityId + "' of type '" + entityType + "' did not exist in database '" + dbId + "'";
                return Response.ok()
                               .entity(msg)
                               .cacheControl(NO_CACHE)
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case PATCH_FAILED:
                msg = "Unable to delete entity '" + entityId + "' of type '" + entityType + "' in database '" + dbId + "'";
                return Response.status(Status.CONFLICT)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
        }
        return unexpected();
    }

    @PATCH
    @Path("{dbid}/t/{type}/e/{ent}")
    @Consumes("application/json-patch+json")
    @Produces(MediaType.TEXT_PLAIN)
    public Response patchEntity(@Context HttpServletRequest request,
                                @Context HttpHeaders headers,
                                @Context UriInfo uri,
                                @PathParam("dbid") String dbId,
                                @PathParam("type") String entityType,
                                @PathParam("ent") String entityId,
                                Array operations) {
        SessionToken token = validateSession(request, dbId);
        EntityId id = Identifier.of(dbId, entityType, entityId);
        Patch<EntityId> patch = Patch.from(id, operations);
        // Check whether the request constrains the entity that will be edited ...
        patch = addConstraints(patch, headers);

        // Submit the patch ...
        EntityChange change = driver.changeEntity(token, patch, defaultTimeout, timeoutUnit);

        // Complete the response ...
        switch (change.status()) {
            case OK:
                return Response.ok()
                               .entity(change.entity().asDocument())
                               .lastModified(timestamp(change.entity().lastModified()))
                               .cacheControl(NO_CACHE)
                               .link(uri.getAbsolutePath(), "remove")
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .tag(Long.toString(change.entity().revision()))
                               .build();
            case DOES_NOT_EXIST:
                String msg = "The entity '" + entityId + "' of type '" + entityType + "' does not exist in database '" + dbId + "'";
                return Response.status(Status.NOT_FOUND)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
            case PATCH_FAILED:
                msg = "Unable to patch entity '" + entityId + "' of type '" + entityType + "' in database '" + dbId + "'";
                return Response.status(Status.CONFLICT)
                               .entity(error(msg, change.failureReasons()))
                               .type(MediaType.APPLICATION_JSON_TYPE)
                               .build();
        }
        return unexpected();
    }

    private <IdType extends Identifier> Patch<IdType> addConstraints(Patch<IdType> patch, HttpHeaders headers) {
        String matchEtag = headers.getHeaderString("If-Match");
        if (matchEtag != null) {
            // Add an operation that tests the special "$revision" field in entities ...
            patch.edit().require(Field.REVISION, Value.create(matchEtag)).end();
        }
        return patch;
    }

    /**
     * Validate the user's session.
     * 
     * @param httpRequest the HTTP request object; may not be null
     * @param databaseId the identifier of the database to which the caller wants to connect; may not be null
     * @return the session token, or null if the user is not authenticated
     */
    private SessionToken validateSession(HttpServletRequest httpRequest, String databaseId) {
        KeycloakSecurityContext session = (KeycloakSecurityContext) httpRequest.getAttribute(KeycloakSecurityContext.class.getName());
        IDToken token = session.getIdToken();
        if (token == null) return null;

        String device = httpRequest.getHeader("X-Debezium-Device");
        String userAgent = httpRequest.getHeader("User-Agent");

        return driver.connect(token.getPreferredUsername(), device, userAgent, databaseId);
    }

    private Date timestamp(long timestamp) {
        if (timestamp <= 0L) return null;
        return Date.from(Instant.ofEpochMilli(timestamp));
    }

    private Document error(String message) {
        return Document.create("message", message);
    }

    private Document error(String message, Collection<String> reasons) {
        Document msg = Document.create();
        msg.setString("message", message);
        msg.setArray("reason", Array.create(reasons));
        return msg;
    }

    private Response unexpected() {
        assert false; // should not get here
        return Response.status(Status.BAD_REQUEST)
                       .entity(error("Unexpected outcome"))
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

}
