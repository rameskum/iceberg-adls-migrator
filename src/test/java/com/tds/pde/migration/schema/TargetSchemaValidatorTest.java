package com.tds.pde.migration.schema;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.dataset.DatasetSchema;
import com.tds.pde.migration.dataset.DatasetSchema.Field;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TargetSchemaValidatorTest {

    private final TargetSchemaValidator validator = new TargetSchemaValidator();

    @Test
    void keepsTheSourceSchemaWhenTheTargetDeclaresNone() {
        Schema source = new Schema(required(1, "id", Types.UUIDType.get()));

        OutputSchema output = validator.resolve(source, null);

        assertThat(output.schema()).isSameAs(source);
        assertThat(output.stringConversionFieldIds()).isEmpty();
    }

    @Test
    void acceptsEquivalentSparkTypesWrittenWithAliasesAndAnyCase() {
        Schema source = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "qty", Types.IntegerType.get()),
                optional(3, "price", Types.DecimalType.of(10, 0)),
                optional(4, "ts", Types.TimestampType.withZone()),
                optional(5, "local_ts", Types.TimestampType.withoutZone()),
                optional(6, "payload", Types.FixedType.ofLength(16)),
                optional(7, "day", Types.DateType.get()));

        OutputSchema output = validator.resolve(source, schema(
                new Field("id", "LONG", false),
                new Field("qty", "Integer", true),
                new Field("price", "decimal", true),
                new Field("ts", "TIMESTAMP", true),
                new Field("local_ts", "timestamp_ntz", true),
                new Field("payload", "binary", true),
                new Field("day", "date", true)));

        assertThat(output.schema().sameSchema(source)).isTrue();
        assertThat(output.stringConversionFieldIds()).isEmpty();
    }

    @Test
    void comparesNestedTypesKeepingFieldNameCase() {
        Schema source = new Schema(
                optional(1, "lines", Types.ListType.ofOptional(2, Types.StructType.of(
                        required(3, "skuId", Types.StringType.get()),
                        optional(4, "amount", Types.DecimalType.of(18, 2))))),
                optional(5, "tags", Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.LongType.get())));

        OutputSchema output = validator.resolve(source, schema(
                new Field("lines", "ARRAY< STRUCT< skuId: STRING, amount: DECIMAL(18, 2) > >", true),
                new Field("tags", "map<string, bigint>", true)));

        assertThat(output.schema().sameSchema(source)).isTrue();
    }

    @Test
    void convertsUuidAndTimeColumnsTheTargetDeclaresAsString() {
        Schema source = new Schema(
                required(1, "id", Types.UUIDType.get()),
                optional(2, "opened_at", Types.TimeType.get()),
                optional(3, "name", Types.StringType.get()));

        OutputSchema output = validator.resolve(source, schema(
                new Field("id", "string", false),
                new Field("opened_at", "string", true),
                new Field("name", "string", true)));

        assertThat(output.stringConversionFieldIds()).containsExactlyInAnyOrder(1, 2);
        Types.NestedField id = output.schema().findField(1);
        assertThat(id.type()).isEqualTo(Types.StringType.get());
        assertThat(id.isRequired()).isTrue();
        assertThat(id.name()).isEqualTo("id");
        assertThat(output.schema().findField(2).type()).isEqualTo(Types.StringType.get());
        assertThat(output.schema().findField(3)).isEqualTo(source.findField(3));
    }

    @Test
    void reportsEveryMismatchAtOnce() {
        Schema source = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "amount", Types.DecimalType.of(18, 2)),
                optional(3, "region", Types.StringType.get()),
                optional(4, "note", Types.StringType.get()));

        assertThatThrownBy(() -> validator.resolve(source, schema(
                new Field("id", "int", false),
                new Field("amount", "decimal(18,2)", true),
                new Field("region", "string", false),
                new Field("extra", "string", true))))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("nothing was written")
                .hasMessageContaining("column 'id' is bigint in the source but int in the target")
                .hasMessageContaining("column 'region' is nullable in the source but not in the target")
                .hasMessageContaining("column 'note' is missing from the target schema")
                .hasMessageContaining("target column 'extra' does not exist in the source");
    }

    @Test
    void refusesToConvertTypesOtherThanUuidAndTime() {
        Schema source = new Schema(required(1, "id", Types.LongType.get()));

        assertThatThrownBy(() -> validator.resolve(source, schema(new Field("id", "string", false))))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("column 'id' is bigint in the source but string in the target");
    }

    @Test
    void acceptsANonNullableSourceColumnDeclaredNullable() {
        Schema source = new Schema(required(1, "id", Types.LongType.get()));

        assertThat(validator.resolve(source, schema(new Field("id", "bigint", true))).schema().sameSchema(source))
                .isTrue();
    }

    @Test
    void reportsTypeStringsThatCannotBeParsed() {
        Schema source = new Schema(required(1, "id", Types.LongType.get()));

        assertThatThrownBy(() -> validator.resolve(source, schema(new Field("id", "big int!", false))))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("column 'id': Not a Spark SQL type: 'big int!'");
    }

    @Test
    void reportsColumnsDeclaredTwice() {
        Schema source = new Schema(required(1, "id", Types.LongType.get()));

        assertThatThrownBy(() -> validator.resolve(source, schema(
                new Field("id", "bigint", false), new Field("id", "bigint", false))))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("target declares column 'id' more than once");
    }

    private static DatasetSchema schema(Field... fields) {
        return new DatasetSchema(List.of(fields));
    }
}
