package org.kafkatwiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ProductorTweetsFicheroTest {

    private MockProducer<String, String> mockProducer;
    private Path tempFile;

    @BeforeEach
    void setUp() throws IOException {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        tempFile = Files.createTempFile("tweets", ".txt");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tempFile);
    }

    /**
     * Este test simula un fichero con tweets correctos y verifica
     * que sólo se envían los que tienen hashtags.
     */
    @Test
    void testTweetsConHashtagsSonEnviados() throws Exception {
        String tweetConHashtag = "{\"text\":\"Hola mundo\",\"lang\":\"es\",\"entities\":{\"hashtags\":[{\"text\":\"Hola\"}]}}";
        String tweetSinHashtag = "{\"text\":\"Sin hashtag\",\"lang\":\"en\",\"entities\":{\"hashtags\":[]}}";

        Files.write(tempFile, List.of(tweetConHashtag, tweetSinHashtag));

        // Ejecutamos una versión modificada del método main
        procesarFicheroYEnviar(mockProducer, tempFile.toString());

        // Verificamos que sólo se haya enviado un tweet (el que tenía hashtags)
        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size(), "Debe enviarse solo un mensaje con hashtags.");

        ProducerRecord<String, String> record = history.get(0);
        assertEquals(ProductorTweetsFichero.TOPIC_NAME, record.topic());
        assertEquals("\"es\"", record.key()); // El campo 'lang' es JSON (entre comillas)
        assertTrue(record.value().contains("\"Hola\""), "El valor debe contener el hashtag 'Hola'.");
    }

    /**
     * Test para verificar que el código ignora líneas no JSON
     * sin lanzar excepciones.
     */
    @Test
    void testLineaNoJsonNoRompeEjecucion() throws Exception {
        Files.write(tempFile, List.of("Esto no es JSON válido"));

        assertDoesNotThrow(() -> procesarFicheroYEnviar(mockProducer, tempFile.toString()));
        assertTrue(mockProducer.history().isEmpty(), "No debe enviarse ningún mensaje.");
    }

    /**
     * Método auxiliar que replica la lógica del main() pero permite
     * inyectar un MockProducer y una ruta de fichero.
     */
    private void procesarFicheroYEnviar(MockProducer<String, String> prod, String rutaFichero) {
        ObjectMapper objectMapper = ProductorTweetsFichero.objectMapper;
        try (BufferedReader br = new BufferedReader(new FileReader(rutaFichero))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    var root = objectMapper.readTree(line);
                    var hashtagsNode = root.path("entities").path("hashtags");
                    if (!hashtagsNode.toString().equals("[]") || !hashtagsNode.toString().equals(" ")) {
                        String value = root.toString();
                        String lang = root.path("lang").toString();
                        prod.send(new ProducerRecord<>(ProductorTweetsFichero.TOPIC_NAME, lang, value));
                    }
                } catch (Exception ignored) {
                    // En el main original se imprime stacktrace, pero aquí lo ignoramos
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
