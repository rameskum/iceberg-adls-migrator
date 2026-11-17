package com.tds.pde.migration.target;

import com.tds.pde.migration.source.SourceFile;
import java.util.Locale;
import org.springframework.stereotype.Component;

/**
 * Names target files: one flat folder, with the partition and source file encoded in the file name,
 * for example {@code region=EU.event_ts_day=2024-01-01__00000-5-a1b2-00001-9f3e1c2a.parquet}.
 *
 * <p>Names are deterministic, so a rerun writes the same file again. The hash of the full source location keeps
 * apart source files that share a file name in different directories.
 */
@Component
public class TargetPathResolver {

    static final int MAX_FILE_NAME_LENGTH = 255;

    private static final String SEPARATOR = "__";
    private static final String EXTENSION = ".parquet";
    private static final int HASH_LENGTH = 8;

    /**
     * @param prefix        folder within the target container, without leading or trailing slash; may be empty
     * @param partitionPath Hive-style source partition path; empty when unpartitioned
     */
    public String resolve(String prefix, String partitionPath, String sourceLocation) {
        String fileName = fileName(partitionPath, sourceLocation);
        return prefix.isEmpty() ? fileName : prefix + "/" + fileName;
    }

    private static String fileName(String partitionPath, String sourceLocation) {
        String suffix = "-" + SourceFile.keyOf(sourceLocation).substring(0, HASH_LENGTH) + EXTENSION;
        String stem = stem(sourceLocation);
        String slug = partitionPath.replace('/', '.');

        int budget = MAX_FILE_NAME_LENGTH - suffix.length();
        if (stem.length() > budget) {
            stem = stem.substring(0, budget);
        }
        String base;
        if (slug.isEmpty()) {
            base = stem;
        } else {
            // Shorten the partition part first: the hash already identifies the file.
            int slugBudget = budget - SEPARATOR.length() - stem.length();
            base = slugBudget > 0
                    ? slug.substring(0, Math.min(slug.length(), slugBudget)) + SEPARATOR + stem
                    : stem;
        }
        // Spark and Hadoop silently skip files whose names start with '_' or '.'.
        if (base.isEmpty() || base.charAt(0) == '_' || base.charAt(0) == '.') {
            base = "p" + base;
            if (base.length() > budget) {
                base = base.substring(0, budget);
            }
        }
        return base + suffix;
    }

    private static String stem(String sourceLocation) {
        String name = sourceLocation.substring(sourceLocation.lastIndexOf('/') + 1);
        return name.toLowerCase(Locale.ROOT).endsWith(EXTENSION)
                ? name.substring(0, name.length() - EXTENSION.length())
                : name;
    }
}
