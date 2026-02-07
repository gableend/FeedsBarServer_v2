import crypto from "crypto";
export function normalizeUrl(raw) {
    try {
        const u = new URL(raw);
        u.hash = "";
        // strip common tracking params
        const drop = new Set([
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "gclid", "fbclid", "mc_cid", "mc_eid"
        ]);
        for (const k of Array.from(u.searchParams.keys())) {
            if (drop.has(k.toLowerCase()))
                u.searchParams.delete(k);
        }
        // stable ordering
        u.searchParams.sort();
        // trim trailing slash for consistency (except root)
        if (u.pathname.length > 1)
            u.pathname = u.pathname.replace(/\/+$/, "");
        return u.toString();
    }
    catch {
        return raw.trim();
    }
}
export function sha256Hex(input) {
    return crypto.createHash("sha256").update(input).digest("hex");
}
export function itemCanonicalHash(feedId, urlNormalized, guid) {
    const key = guid ? `${feedId}|guid|${guid}` : `${feedId}|url|${urlNormalized}`;
    return sha256Hex(key);
}
export function normalizeTitle(t) {
    if (!t)
        return null;
    return t.trim().toLowerCase().replace(/\s+/g, " ");
}
