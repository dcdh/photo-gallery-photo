package com.redhat.photogallery.photo;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.eventbus.MessageConsumer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertAll;

@QuarkusTest
public class PhotoServiceTest {

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @BeforeEach
    public void setup() {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement truncateStatement = connection.prepareStatement(
                     "TRUNCATE TABLE PhotoItem")) {
            truncateStatement.execute();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldCreatePhoto() {
        // Given
        // language=json
        final String givenPayload = """
                {
                    "id": 1,
                    "category": "animals",
                    "name": "Calinou"
                }
                """;

        // When & Then
        final String id = given()
                .body(givenPayload)
                .contentType(ContentType.JSON)
                .when()
                .post("/photos")
                .then()
                .log().all()
                .statusCode(200)
                .body(notNullValue())
                .extract()
                .body()
                .asString();
        assertThat(id).isEqualTo("1");

        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement countPreparedStatement = connection.prepareStatement(
                     "SELECT COUNT(*) AS count FROM PhotoItem");
             final PreparedStatement selectPhotosPreparedStatement = connection.prepareStatement(
                     "SELECT * FROM PhotoItem")) {
            final ResultSet countResultSet = countPreparedStatement.executeQuery();
            countResultSet.next();
            assertThat(countResultSet.getInt("count")).isEqualTo(1);

            final ResultSet photosResultSet = selectPhotosPreparedStatement.executeQuery();
            photosResultSet.next();
            assertAll(
                    () -> assertThat(photosResultSet.getLong("id")).isEqualTo(1L),
                    () -> assertThat(photosResultSet.getString("category")).isEqualTo("animals"),
                    () -> assertThat(photosResultSet.getString("name")).isEqualTo("Calinou")
            );
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldPublishMessageWhenCreatingPhoto() {
        // Given
        final MessageConsumer<JsonObject> givenTopic = eventBus.consumer("photos");
        final List<JsonObject> messagesConsumed = new ArrayList<>();
        givenTopic.handler(json -> messagesConsumed.add(json.body()));
        // language=json
        final String givenPayload = """
                {
                    "id": 1,
                    "category": "animals",
                    "name": "Calinou"
                }
                """;

        // When
        given()
                .body(givenPayload)
                .contentType(ContentType.JSON)
                .when()
                .post("/photos")
                .then()
                .log().all()
                .statusCode(200);

        // Then
        assertAll(
                () -> assertThat(messagesConsumed.get(0).getLong("id")).isEqualTo(1L),
                () -> assertThat(messagesConsumed.get(0).getString("category")).isEqualTo("animals"),
                () -> assertThat(messagesConsumed.get(0).getString("name")).isEqualTo("Calinou"));

    }

    @Test
    public void shouldReadAllPhotos() {
        // Given
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement truncateStatement = connection.prepareStatement(
                     "INSERT INTO PhotoItem(id, category, name) VALUES (1000, 'animals', 'Calinou')")) {
            truncateStatement.execute();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        // When & Then
        given()
                .get("/photos")
                .then()
                .statusCode(200)
                .body("size()", is(1))
                .body("[0].id", is(1000))
                .body("[0].category", is("animals"))
                .body("[0].name", is("Calinou"));
    }

}
