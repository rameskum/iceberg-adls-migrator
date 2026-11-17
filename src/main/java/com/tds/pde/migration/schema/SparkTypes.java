package com.tds.pde.migration.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Translates between Iceberg types and the Spark SQL type strings used in dataset schemas,
 * so both sides can be compared as canonical strings.
 */
final class SparkTypes {

    private static final Pattern TOKEN = Pattern.compile("\\s*(?:([A-Za-z_][A-Za-z0-9_]*)|(\\d+)|([<>,:()]))");

    private static final Map<String, String> ALIASES = Map.of(
            "long", "bigint",
            "integer", "int",
            "short", "smallint",
            "byte", "tinyint",
            "real", "float",
            "dec", "decimal",
            "numeric", "decimal",
            "timestamp_ltz", "timestamp");

    private SparkTypes() {
    }

    /** The Spark SQL type Databricks reads this Iceberg type as, or the Iceberg name when Spark has none. */
    static String render(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> "boolean";
            case INTEGER -> "int";
            case LONG -> "bigint";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case DATE -> "date";
            case TIMESTAMP -> ((Types.TimestampType) type).shouldAdjustToUTC() ? "timestamp" : "timestamp_ntz";
            case STRING -> "string";
            case FIXED, BINARY -> "binary";
            case DECIMAL -> {
                Types.DecimalType decimal = (Types.DecimalType) type;
                yield "decimal(" + decimal.precision() + "," + decimal.scale() + ")";
            }
            case STRUCT -> type.asStructType().fields().stream()
                    .map(field -> field.name() + ":" + render(field.type()))
                    .collect(Collectors.joining(",", "struct<", ">"));
            case LIST -> "array<" + render(type.asListType().elementType()) + ">";
            case MAP -> "map<" + render(type.asMapType().keyType()) + "," + render(type.asMapType().valueType()) + ">";
            default -> type.toString();
        };
    }

    /** Whether Spark cannot read the Parquet type Iceberg writes for this type. */
    static boolean isSparkUnreadable(Type type) {
        return switch (type.typeId()) {
            case UUID, TIME, TIMESTAMP_NANO -> true;
            case STRUCT -> type.asStructType().fields().stream().anyMatch(field -> isSparkUnreadable(field.type()));
            case LIST -> isSparkUnreadable(type.asListType().elementType());
            case MAP -> isSparkUnreadable(type.asMapType().keyType()) || isSparkUnreadable(type.asMapType().valueType());
            default -> false;
        };
    }

    /**
     * Canonical form of a Spark SQL type string: keywords lower-cased with aliases resolved, whitespace removed,
     * struct field names kept as written.
     *
     * @throws IllegalArgumentException when the string is not a type expression
     */
    static String normalize(String sparkType) {
        List<String> tokens = tokenize(sparkType);
        StringBuilder canonical = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            String next = i + 1 < tokens.size() ? tokens.get(i + 1) : "";
            if (!Character.isLetter(token.charAt(0)) && token.charAt(0) != '_') {
                canonical.append(token);
            } else if (next.equals(":")) {
                canonical.append(token);
            } else {
                String keyword = token.toLowerCase();
                keyword = ALIASES.getOrDefault(keyword, keyword);
                // Spark's bare DECIMAL means DECIMAL(10,0).
                canonical.append(keyword.equals("decimal") && !next.equals("(") ? "decimal(10,0)" : keyword);
            }
        }
        return canonical.toString();
    }

    private static List<String> tokenize(String sparkType) {
        List<String> tokens = new ArrayList<>();
        Matcher matcher = TOKEN.matcher(sparkType);
        int position = 0;
        while (position < sparkType.length() && matcher.find(position) && matcher.start() == position) {
            tokens.add(matcher.group(1) != null ? matcher.group(1)
                    : matcher.group(2) != null ? matcher.group(2) : matcher.group(3));
            position = matcher.end();
        }
        if (!sparkType.substring(position).isBlank() || tokens.isEmpty()) {
            throw new IllegalArgumentException("Not a Spark SQL type: '" + sparkType + "'");
        }
        return tokens;
    }
}
