package com.tds.pde.migration.config;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.util.unit.DataSize;

/**
 * The annotated {@link DataSize} must lie within {@code min} and {@code max} bytes, inclusive. {@code null} is valid.
 */
@Documented
@Constraint(validatedBy = DataSizeRange.Validator.class)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
public @interface DataSizeRange {

    long min() default 0;

    long max() default Long.MAX_VALUE;

    String message() default "must be between {min} and {max} bytes";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<DataSizeRange, DataSize> {

        private long min;
        private long max;

        @Override
        public void initialize(DataSizeRange range) {
            this.min = range.min();
            this.max = range.max();
        }

        @Override
        public boolean isValid(DataSize value, ConstraintValidatorContext context) {
            return value == null || value.toBytes() >= min && value.toBytes() <= max;
        }
    }
}
