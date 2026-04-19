package org.dataflow.validator;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class SecretResolver {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{secret:([a-zA-Z0-9_.-]+)}");

    private final Environment env;

    public SecretResolver(Environment env) {
        this.env = env;
    }

    public String resolve(String input) {
        if (input == null || input.indexOf("${secret:") < 0) {
            return input;
        }
        Matcher m = PLACEHOLDER.matcher(input);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            String value = env.getProperty("dataflow.secrets." + key);
            if (value == null) {
                throw new IllegalStateException("Unresolved secret reference: " + key);
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }
}
