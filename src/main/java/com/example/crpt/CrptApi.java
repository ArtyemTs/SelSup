import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe клиент для работы с API «Честный Знак» (ГИС МТ).
 * <p>
 * Класс реализует один публичный метод создания документа «Ввод в оборот. Производство РФ"
 * (тип LP_INTRODUCE_GOODS) через единый метод создания документов
 * {@code POST /api/v3/lk/documents/create?pg=<productGroup>}.
 * <p>
 * Основные особенности реализации:
 * Потокобезопасность: использование неизменяемого состояния и собственный блокирующий
 * ограничитель запросов (rate limiter), который обеспечивает не более N запросов за окно времени.</li>
 * Ограничение по числу запросов: при исчерпании лимита поток блокируется до момента,
 * когда отправка станет возможной (без busy-waiting).</li>
 * Расширяемость: вынесен {@link TokenProvider} для гибкой поставки токена, предусмотрены
 * enum/классы моделей запроса, которые легко расширять новыми полями и документами.</li>
 * Стандартный HTTP-клиент Java 11 (java.net.http) и Jackson для (де)сериализации JSON.</li>
 * <p>
 * ВАЖНО: Для работы методов API в заголовке запроса требуется передавать
 * {@code Authorization: Bearer <token>}. Время жизни токена и способы его получения
 * зависят от внешней аутентификации и вне скоупа этой задачи.
 */
public final class CrptApi {

    private final HttpClient httpClient;
    private final URI baseUri;
    private final ObjectMapper mapper;
    private final RateLimiter rateLimiter;

    private final TokenProvider tokenProvider;
    private final String defaultProductGroup;

    private CrptApi(Builder builder) {
        this.httpClient = builder.httpClient;
        this.mapper = builder.objectMapper;
        this.tokenProvider = builder.tokenProvider;
        this.defaultProductGroup = builder.defaultProductGroup;
        this.rateLimiter = builder.rateLimiter;
        this.baseUri = builder.baseUri;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Создание документа «Ввод в оборот. Производство РФ».
     * <p>
     * Документ сериализуется в JSON, затем кодируется в Base64 и отправляется в теле запроса
     * единым методом создания документов:
     * {@code POST /api/v3/lk/documents/create?pg=<productGroup>}
     * с типом {@code LP_INTRODUCE_GOODS} и {@code document_format = MANUAL}.
     * <p>
     * Ссылки на документацию (Opisanie-API-GIS-MP.pdf):
     * - Раздел «Единый метод создания документов», URL /api/v3/lk/documents/create — параметры запроса (pg), поля тела: document_format, product_document, signature, type. Стр. 45.
     * - Раздел «2.2.4.1 Ввод в оборот товара, произведенного на территории РФ (LP_INTRODUCE_GOODS)» — структура JSON документа. Стр. 109–110.
     * <p>
     *
     * @param document                тело документа LP_INTRODUCE_GOODS в виде Java-объекта
     * @param detachedSignatureBase64 откреплённая подпись под документом, в Base64-строке
     * @return UUID созданного документа в ГИС МТ
     * @throws CrptApiException     если сервер вернул ошибку или ответ некорректен
     * @throws IOException          сетевые/IO ошибки
     * @throws InterruptedException если поток был прерван во время ожидания лимитера/HTTP
     */
    public UUID createDocumentRF(Document document,
                                 String detachedSignatureBase64)
            throws CrptApiException, IOException, InterruptedException {
        // Используем товарную группу по умолчанию, если задана
        final String pg = this.defaultProductGroup;
        if (pg == null || pg.isBlank()) {
            throw new IllegalStateException("Product group is required. Call withDefaultProductGroup(...) or use the overload with explicit productGroup.");
        }
        return createDocumentRF(document, detachedSignatureBase64, pg);
    }

    /**
     * Перегрузка с явной товарной группой (pg), если нет значения по умолчанию.
     */
    public UUID createDocumentRF(Document document,
                                 String detachedSignatureBase64,
                                 String productGroup)
            throws CrptApiException, IOException, InterruptedException {
        // Валидация входных параметров согласно требованиям спецификации (см. ссылки в Javadoc)
        validateInputs(document, detachedSignatureBase64, productGroup);
        final String token = requireToken();

        rateLimiter.acquire();

        final String productDocJson = mapper.writeValueAsString(document);
        // Кодирование JSON в Base64 — согласно требованию «product_document: Base64(JSON.stringify)»
        final String productDocB64 = Base64.getEncoder().encodeToString(productDocJson.getBytes(StandardCharsets.UTF_8));
        final CreateRequest body = new CreateRequest(
                "MANUAL",                          // формат json согласно документации
                productDocB64,
                detachedSignatureBase64,
                "LP_INTRODUCE_GOODS",             // тип документа: ввод в оборот, производство РФ
                productGroup
        );
        final String jsonBody = mapper.writeValueAsString(body);

        // Параметр pg передаётся как query-параметр (обязательный в методе create (см. ссылки в Javadoc))
        final String encodedPg = URLEncoder.encode(productGroup, StandardCharsets.UTF_8.name());
        final URI uri = baseUri.resolve("/api/v3/lk/documents/create?pg=" + encodedPg);

        final HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Authorization", "Bearer " + token) // обязательный заголовок авторизации
                .header("Content-Type", "application/json")  // тело запроса — JSON
                .header("Accept", "application/json")
                .header("User-Agent", "CrptApi/1.0")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        final HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        final int code = resp.statusCode();
        if (code / 100 == 2) {
            final CreateResponse ok = mapper.readValue(resp.body(), CreateResponse.class);
            if (ok.value == null || ok.value.isBlank()) {
                throw new CrptApiException("Server returned 2xx but 'value' is empty");
            }
            try {
                return UUID.fromString(ok.value);
            } catch (IllegalArgumentException ex) {
                throw new CrptApiException("Unexpected 'value' format: " + ok.value, ex);
            }
        }

        String message = parseErrorMessage(resp.body());
        throw new CrptApiException("HTTP " + code + ": " + message);
    }

    private String parseErrorMessage(String body) {
        if (body == null || body.isBlank()) {
            return "Empty response";
        }
        try {
            final ErrorResponse err = mapper.readValue(body, ErrorResponse.class);
            if (err.errorMessage != null && !err.errorMessage.isBlank()) {
                return err.errorMessage;
            }
            if (err.error_message != null && !err.error_message.isBlank()) {
                return err.error_message;
            }
        } catch (IOException ignore) {
            // Тело может быть не JSON (например, XML) — вернём первые символы как есть
        }
        return body.length() > 500 ? body.substring(0, 500) + "…" : body;
    }

    private static String ensureNoTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private String requireToken() {
        final TokenProvider tp = this.tokenProvider;
        if (tp == null) {
            throw new IllegalStateException("Token is not set. Use withToken(...) or withTokenProvider(...)");
        }
        final String token = tp.getToken();
        if (token == null || token.isBlank()) {
            throw new IllegalStateException("Token provider returned empty token");
        }
        return token;
    }

// ====== ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ ======

    /**
     * Клиентская валидация согласно спецификации Opisanie-API-GIS-MP.pdf:
     * - обязательность pg и полей body (document_format/product_document/signature/type) для метода create (стр. 45);
     * - обязательные поля документа LP_INTRODUCE_GOODS: owner_inn, participant_inn, producer_inn, production_date (YYYY-MM-DD),
     * production_type, products[], и для каждого товара tnved_code + одно из uit_code/uitu_code (стр. 109–110).
     * Цель — вернуть осмысленную ошибку ещё до вызова API.
     */
    private void validateInputs(Document doc, String signatureB64, String productGroup) throws ValidationException {
        List<String> errors = new ArrayList<>();
        if (isBlank(productGroup)) {
            errors.add("productGroup (query param 'pg') is required");
        }
        if (isBlank(signatureB64)) {
            errors.add("signature (detached Base64) is required");
        } else {
            try {
                Base64.getDecoder().decode(signatureB64);
            } catch (IllegalArgumentException e) {
                errors.add("signature must be valid Base64");
            }
        }
        if (doc == null) {
            errors.add("document is required");
        } else {
            if (isBlank(doc.ownerInn)) {
                errors.add("document.owner_inn is required");
            }
            if (isBlank(doc.participantInn)) {
                errors.add("document.participant_inn is required");
            }
            if (isBlank(doc.producerInn)) {
                errors.add("document.producer_inn is required");
            }
            if (isBlank(doc.productionDate)) {
                errors.add("document.production_date is required");
            } else if (!isValidDate(doc.productionDate)) {
                errors.add("document.production_date must be YYYY-MM-DD");
            }
            if (isBlank(doc.productionType)) {
                errors.add("document.production_type is required");
            }
            if (doc.products == null || doc.products.isEmpty()) {
                errors.add("document.products must not be empty");
            } else {
                int i = 0;
                for (Product p : doc.products) {
                    if (p == null) {
                        errors.add("document.products[" + i + "] must not be null");
                        i++;
                        continue;
                    }
                    if (isBlank(p.tnvedCode)) {
                        errors.add("products[" + i + "].tnved_code is required");
                    }
                    if (isBlank(p.uitCode) && isBlank(p.uituCode)) {
                        errors.add("products[" + i + "]: either uit_code or uitu_code is required");
                    }
                    if (!isBlank(p.productionDate) && !isValidDate(p.productionDate)) {
                        errors.add("products[" + i + "].production_date must be YYYY-MM-DD when present");
                    }
                    if (!isBlank(p.certificateDocumentDate) && !isValidDate(p.certificateDocumentDate)) {
                        errors.add("products[" + i + "].certificate_document_date must be YYYY-MM-DD when present");
                    }
                    i++;
                }
            }
        }
        if (!errors.isEmpty()) {
            throw new ValidationException("Validation failed: " + String.join("; ", errors));
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static boolean isValidDate(String s) {
        // Проверка формата YYYY-MM-DD
        return s.matches("\\d{4}-\\d{2}-\\d{2}");
    }

    public static class ValidationException extends CrptApiException {
        public ValidationException(String message) {
            super(message);
        }
    }

// ====== МОДЕЛИ ЗАПРОСА/ОТВЕТА (внутренние классы для единичного файла) ======

    /**
     * Тело запроса для единого метода создания документов.
     * См. PDF: раздел «Единый метод создания документов», стр. 45 (URL: /api/v3/lk/documents/create; поля document_format, product_document, signature, type; query-параметр pg).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    static final class CreateRequest {
        @JsonProperty("document_format")
        public final String documentFormat;     // MANUAL | XML | CSV (мы используем MANUAL — JSON)
        @JsonProperty("product_document")
        public final String productDocument;    // Base64(JSON)
        @JsonProperty("signature")
        public final String signature;          // Открепленная подпись в Base64
        @JsonProperty("type")
        public final String type;               // LP_INTRODUCE_GOODS
        @JsonProperty("product_group")
        public final String productGroup;       // дублируется для совместимости. Основное в query param pg

        CreateRequest(String documentFormat, String productDocument, String signature, String type, String productGroup) {
            this.documentFormat = documentFormat;
            this.productDocument = productDocument;
            this.signature = signature;
            this.type = type;
            this.productGroup = productGroup;
        }
    }

    static final class CreateResponse {
        public String value;
    }

    static final class ErrorResponse {
        @JsonProperty("error_message")
        public String error_message; // часто встречающееся имя
        @JsonProperty("errorMessage")
        public String errorMessage;   // на всякий случай
    }

// ====== МОДЕЛЬ ДОКУМЕНТА LP_INTRODUCE_GOODS (Производство РФ) ======
// Вынесена во внутренние классы, чтобы файл оставался единичным и легко расширяемым.

    /**
     * Модель документа «Ввод в оборот. Производство РФ» (LP_INTRODUCE_GOODS) согласно спецификации.
     * См. PDF: раздел «2.2.4.1 Ввод в оборот товара, произведенного на территории РФ», стр. 109–110.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Document {
        public static final class Description {
            @JsonProperty("participantInn")
            public String participantInn;
        }

        @JsonProperty("description")
        public Description description;

        @JsonProperty("doc_id")
        public String docId;                    // опционально (пример из спецификации)
        @JsonProperty("doc_status")
        public String docStatus;                // опционально
        @JsonProperty("doc_type")
        public String docType;                  // опционально

        @JsonProperty("importRequest")
        public Boolean importRequest;           // опционально

        @JsonProperty("owner_inn")
        public String ownerInn;                 // ИНН собственника
        @JsonProperty("participant_inn")
        public String participantInn;           // ИНН участника оборота
        @JsonProperty("producer_inn")
        public String producerInn;              // ИНН производителя
        @JsonProperty("production_date")
        public String productionDate;           // YYYY-MM-DD
        @JsonProperty("production_type")
        public String productionType;           // строковое значение из спецификации

        @JsonProperty("products")
        public List<Product> products;          // массив товаров
    }

    /**
     * Элемент массива products.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Product {
        @JsonProperty("certificate_document")
        public String certificateDocument;          // вид документа обязательной сертификации
        @JsonProperty("certificate_document_date")
        public String certificateDocumentDate;      // YYYY-MM-DD
        @JsonProperty("certificate_document_number")
        public String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        public String ownerInn;
        @JsonProperty("producer_inn")
        public String producerInn;
        @JsonProperty("production_date")
        public String productionDate;               // YYYY-MM-DD
        @JsonProperty("tnved_code")
        public String tnvedCode;
        @JsonProperty("uit_code")
        public String uitCode;                      // КИ
        @JsonProperty("uitu_code")
        public String uituCode;                     // КИТУ
    }

    /**
     * Справочник товарных групп (значения query-параметра pg в /lk/documents/create),
     * см. PDF: раздел «Единый метод создания документов», таблица параметров запроса (product_group), стр. 45,
     */
    public enum ProductGroup {
        CLOTHES("clothes"), SHOES("shoes"), TOBACCO("tobacco"), PERFUMERY("perfumery"), TIRES("tires"),
        ELECTRONICS("electronics"), PHARMA("pharma"), MILK("milk"), BICYCLE("bicycle"), WHEELCHAIRS("wheelchairs");
        public final String value;

        ProductGroup(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

// ====== РЕАЛИЗАЦИЯ БЛОКИРУЮЩЕГО RATE LIMITER ======

    /**
     * Блокирующий лимитер: не более N запросов за 1 интервал (TimeUnit).
     * Потокобезопасный. Строгое «скользящее окно».
     */
    static final class RateLimiter {
        private final int limit;
        private final long windowNanos;
        private final Deque<Long> timestamps = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock(true);
        private final Condition slotFreed = lock.newCondition();

        RateLimiter(int limit, TimeUnit unit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be > 0");
            }
            if (unit == null) {
                throw new IllegalArgumentException("unit must not be null");
            }
            this.limit = limit;
            this.windowNanos = unit.toNanos(1);
        }

        /**
         * Блокирует до момента, когда новый запрос можно отправить, и регистрирует его.
         */
        void acquire() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                for (; ; ) {
                    long now = System.nanoTime();
                    trim(now); // батч-очистка «устаревших» меток
                    if (timestamps.size() < limit) {
                        timestamps.addLast(now);
                        return;
                    }
                    long oldest = timestamps.peekFirst(); // не null, т.к. size >= limit
                    long waitNanos = windowNanos - (now - oldest);
                    if (waitNanos <= 0L) {
                        continue;
                    }
                    slotFreed.awaitNanos(waitNanos);
                }
            } finally {
                lock.unlock();
            }
        }

        private void trim(long now) {
            int removed = 0;
            while (!timestamps.isEmpty()) {
                long ts = timestamps.peekFirst();
                if (now - ts >= windowNanos) {
                    timestamps.pollFirst();
                    removed++;
                } else {
                    break;
                }
            }
            if (removed > 0) {
                slotFreed.signalAll();
            }
        }
    }

    @FunctionalInterface
    public interface TokenProvider {
        String getToken();
    }

    public static final class FixedTokenProvider implements TokenProvider {
        private final AtomicReference<String> token = new AtomicReference<>();

        public FixedTokenProvider() {
        }

        public FixedTokenProvider(String initialToken) {
            token.set(initialToken);
        }

        public void setToken(String newToken) {
            token.set(Objects.requireNonNull(newToken));
        }

        @Override
        public String getToken() {
            String t = token.get();
            if (t == null) throw new IllegalStateException("Token not set");
            return t;
        }
    }

    public static class CrptApiException extends Exception {
        public CrptApiException(String message) {
            super(message);
        }

        public CrptApiException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class Builder {
        private HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
        private ObjectMapper objectMapper = defaultMapper();
        private TokenProvider tokenProvider = new FixedTokenProvider();
        private String defaultProductGroup;
        private RateLimiter rateLimiter = new RateLimiter(100, TimeUnit.MINUTES);
        private URI baseUri = URI.create("https://ismp.crpt.ru");

        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient);
            return this;
        }

        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = Objects.requireNonNull(objectMapper);
            return this;
        }

        public Builder tokenProvider(TokenProvider tokenProvider) {
            this.tokenProvider = Objects.requireNonNull(tokenProvider);
            return this;
        }

        public Builder defaultProductGroup(String defaultProductGroup) {
            this.defaultProductGroup = Objects.requireNonNull(defaultProductGroup);
            return this;
        }

        public Builder rateLimiter(RateLimiter rateLimiter) {
            this.rateLimiter = Objects.requireNonNull(rateLimiter);
            return this;
        }

        public Builder baseUri(String baseUri) {
            this.baseUri = URI.create(baseUri);
            return this;
        }

        public CrptApi build() {
            Objects.requireNonNull(defaultProductGroup, "defaultProductGroup must be set");
            return new CrptApi(this);
        }
    }

    private static ObjectMapper defaultMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    // ===================
// Пример использования:
// ===================
//    ObjectMapper mapper = new ObjectMapper()
//            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//
//    // Потокобезопасный провайдер токена (можно ротировать без мутаций клиента)
//    CrptApi.FixedTokenProvider tokens = new CrptApi.FixedTokenProvider("<JWT>");
//
//    CrptApi api = CrptApi.builder()
//            .tokenProvider(tokens)
//            .defaultProductGroup(CrptApi.ProductGroup.MILK.toString())
//            .objectMapper(mapper)
//            .rateLimiter(new CrptApi.RateLimiter(100, TimeUnit.MINUTES))
//            .baseUri("https://ismp.crpt.ru") // Базовый прод-адрес ГИС МТ согласно документации.
//            .build();
//
//    // Формируем документ (DTO как раньше — поля публичные)
//    CrptApi.Document doc = new CrptApi.Document();
//    doc.ownerInn ="1234567890";
//    doc.producerInn ="1234567890";
//    doc.productionDate ="2020-01-23";
//    doc.productionType ="LOCAL"; // пример, заполните согласно справочнику вашей ТГ
//    doc.products =new ArrayList<>();
//
//    CrptApi.Product p = new CrptApi.Product();
//    p.tnvedCode ="6401100000";
//    p.uitCode ="010463003407002921wskg1E44R1qym2406401";
//        doc.products.add(p);
//
//    // Отправка
//    UUID id = api.createDocumentRF(doc, "<detached-signature-base64>");
//        System.out.println("Created doc: "+id);
//
//// При необходимости можно безопасно ротировать токен:
//        tokens.setToken("<NEW_JWT>");
}
