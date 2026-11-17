package com.tds.pde.migration.dataset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Schema declared for a dataset, with column types written as Spark SQL type strings
 * such as {@code bigint}, {@code decimal(18,2)} or {@code array<struct<id:int,name:string>>}.
 */
public record DatasetSchema(List<Field> fields) {

    public DatasetSchema {
        fields = fields == null ? List.of() : List.copyOf(fields);
    }

    /**
     * @param nullable defaults to {@code true}, as in Spark, when the descriptor omits it
     */
    public record Field(String name, String type, boolean nullable) {

        @JsonCreator
        public Field(@JsonProperty("name") String name, @JsonProperty("type") String type,
                     @JsonProperty("nullable") @Nullable Boolean nullable) {
            this(name, type, nullable == null || nullable);
        }

        public Field {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("schema field name is required");
            }
            if (type == null || type.isBlank()) {
                throw new IllegalArgumentException("type is required for schema field " + name);
            }
        }
    }
}
