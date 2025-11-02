package org.kafkatwiter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TweetsHashtagsCounterTest {

    @Test
    void testGetHashtags_ConHashtags() {
        // JSON con hashtags
        String tweetJson = """
            {
              "text": "Hola mundo",
              "entities": {
                "hashtags": [
                  {"tag": "Hola"},
                  {"tag": "Mundo"}
                ]
              }
            }
            """;

        String hashtag = TweetsHashtagsCounter.getHashtags(tweetJson);

        assertEquals("Hola", hashtag, "Debe devolver el primer hashtag del array");
    }

    @Test
    void testGetHashtags_SinHashtags() {
        // JSON sin hashtags
        String tweetJson = """
            {
              "text": "Tweet sin hashtags",
              "entities": {
                "hashtags": []
              }
            }
            """;

        String hashtag = TweetsHashtagsCounter.getHashtags(tweetJson);

        assertEquals("", hashtag, "Debe devolver cadena vacía si no hay hashtags");
    }

    @Test
    void testGetHashtags_SinCampoEntities() {
        // JSON sin campo entities
        String tweetJson = """
            {
              "text": "Tweet inválido"
            }
            """;

        String hashtag = TweetsHashtagsCounter.getHashtags(tweetJson);

        assertEquals("", hashtag, "Debe devolver cadena vacía si falta el campo entities");
    }

    @Test
    void testGetHashtags_JsonInvalido() {
        // Entrada no JSON
        String invalidJson = "Esto no es JSON válido";

        assertDoesNotThrow(() -> {
            String hashtag = TweetsHashtagsCounter.getHashtags(invalidJson);
            assertEquals("", hashtag, "Debe devolver cadena vacía ante JSON inválido");
        });
    }
}
