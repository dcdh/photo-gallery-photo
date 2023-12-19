package com.redhat.photogallery.photo;

import com.redhat.photogallery.common.Constants;
import com.redhat.photogallery.common.data.PhotoCreatedMessage;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.eventbus.MessageProducer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

@Path("/photos")
public class PhotoService {
    private static final Logger LOG = LoggerFactory.getLogger(PhotoService.class);
    private final MessageProducer<JsonObject> topic;
    private final EntityManager entityManager;

    public PhotoService(final EntityManager entityManager, final EventBus eventBus) {
        this.entityManager = Objects.requireNonNull(entityManager);
        this.topic = eventBus.publisher(Constants.PHOTOS_TOPIC_NAME);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Transactional
    public Long createPhoto(final PhotoItem item) {
        item.persist();
        LOG.info("Added {} into the data store", item);

        final PhotoCreatedMessage message = new PhotoCreatedMessage(item.id, item.name, item.category);
        topic.write(JsonObject.mapFrom(message))
                .subscribe()
                .with(onItemCallback -> LOG.info("Published {} on topic {}", message, topic.address()));

        return item.id;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response readAllPhotos() {
        final TypedQuery<PhotoItem> query = entityManager.createQuery("FROM PhotoItem", PhotoItem.class);
        final List<PhotoItem> items = query.getResultList();
        LOG.info("Returned all {} items", items.size());
        return Response.ok(new GenericEntity<>(items) {
        }).build();
    }

}