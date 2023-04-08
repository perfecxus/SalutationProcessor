package org.example.validators;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class CustomValidators {

    public static final Validator BEFORE_OR_AFTER_VALIDATOR = new Validator() {
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            String reason = null;
            if (!"B".equalsIgnoreCase(value) && !"A".equalsIgnoreCase(value)) {
                reason = String.format("Unsuported value present: '%s', supported values are:[A,B,a,b]", value);
            }
            return (new ValidationResult.Builder()).subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }

    };
}
