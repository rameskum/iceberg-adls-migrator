package com.tds.pde.migration.target.azure;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A target given as {@code abfss://<container>@<account>.dfs.<endpoint-suffix>/<path>}.
 *
 * <p>Without hierarchical namespace the account is plain Blob Storage, so the data is written through the
 * {@code blob} endpoint rather than {@code dfs}.
 *
 * @param prefix   path within the container, without leading or trailing slash; empty for the container root
 * @param location the location as given, for messages
 */
public record AzureLocation(String account, String container, String prefix, String blobEndpoint, String location) {

    private static final Pattern HOST = Pattern.compile("([a-z0-9]{3,24})\\.dfs\\.(.+)");

    public static AzureLocation parse(String location) {
        URI uri;
        try {
            uri = new URI(location);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid target location '" + location + "'", e);
        }
        if (!"abfss".equals(uri.getScheme()) && !"abfs".equals(uri.getScheme())) {
            throw new IllegalArgumentException("Target location '" + location
                    + "' must be abfss://<container>@<account>.dfs.core.windows.net/<path>");
        }
        String container = uri.getUserInfo();
        String host = uri.getHost();
        if (container == null || container.isBlank() || host == null) {
            throw new IllegalArgumentException("Target location '" + location
                    + "' must name a container and an account: abfss://<container>@<account>.dfs.core.windows.net");
        }
        Matcher matcher = HOST.matcher(host);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Target location '" + location
                    + "' must use a dfs endpoint such as <account>.dfs.core.windows.net");
        }
        String path = uri.getPath() == null ? "" : uri.getPath();
        String prefix = path.replaceAll("^/+|/+$", "");
        if (prefix.contains("//")) {
            throw new IllegalArgumentException("Target location '" + location + "' has an empty path segment");
        }
        String account = matcher.group(1);
        return new AzureLocation(account, container, prefix,
                "https://" + account + ".blob." + matcher.group(2), location);
    }

    public boolean isContainerRoot() {
        return prefix.isEmpty();
    }

    /** The start of the name of every blob under this location: the prefix and a slash, or empty at the root. */
    public String blobPrefix() {
        return prefix.isEmpty() ? "" : prefix + "/";
    }

    /**
     * Whether this location and {@code other} are the same folder, or one is within the other, in the same
     * container: then cleaning either deletes files of the other.
     */
    public boolean overlaps(AzureLocation other) {
        return blobEndpoint.equals(other.blobEndpoint) && container.equals(other.container)
                && (isWithin(prefix, other.prefix) || isWithin(other.prefix, prefix));
    }

    /** Whether the folder {@code inner} is {@code outer} or within it; blob names are case-sensitive. */
    private static boolean isWithin(String inner, String outer) {
        return outer.isEmpty() || inner.equals(outer) || inner.startsWith(outer + "/");
    }

    /** Joins a name under this location's prefix. */
    public String blobName(String relativeName) {
        return prefix.isEmpty() ? relativeName : prefix + "/" + relativeName;
    }

    @Override
    public String toString() {
        return location;
    }
}
