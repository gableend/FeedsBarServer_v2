import express from "express";
import { getSupabaseAdmin, parser, findImage, isFatalError, getSmartIconUrl, normalizeUrl, itemCanonicalHash, normalizeTitle, nowIso } from "@feedsbar/shared";

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || "8080";
const BATCH_SIZE = Number(process.env.INGEST_BATCH_SIZE || 10);
const MAX_ITEMS_PER_FEED = Number(process.env.MAX_ITEMS_PER_FEED || 50);
const RETENTION_DAYS = Number(process.env.RETENTION_DAYS || 90);
const DEFAULT_POLL_MINUTES = Number(process.env.DEFAULT_POLL_MINUTES || 30);

const supabase = getSupabaseAdmin();

app.get("/health", (_req, res) => res.status(200).send("ok"));

app.post("/jobs/ingest", async (_req, res) => {
  const started = nowIso();
  const log: any = { ok: true, started, processed: 0, errors: 0 };

  try {
    // 1) pick due feeds (simple MLP selection)
    const { data: feeds, error: feedsErr } = await supabase
      .from("feeds")
      .select("id,url,name,icon_url,is_active,last_fetched_at,consecutive_error_count,poll_interval_minutes,next_poll_at,status")
      .eq("is_active", true)
      .or("next_poll_at.is.null,next_poll_at.lte.now()")
      .order("next_poll_at", { ascending: true, nullsFirst: true })
      .limit(BATCH_SIZE);

    if (feedsErr) throw feedsErr;

    for (const feed of feeds ?? []) {
      try {
        // parse feed (keep your proven parser config)
        const feedData: any = await parser.parseURL(feed.url);

        // opportunistically set icon if missing (keep your proven logic)
        if (!feed.icon_url) {
          const homepage = feedData?.link || feed.url;
          const icon = await getSmartIconUrl(homepage);
          if (icon) {
            await supabase.from("feeds").update({ icon_url: icon }).eq("id", feed.id);
          }
        }

        const items = Array.isArray(feedData?.items) ? feedData.items.slice(0, MAX_ITEMS_PER_FEED) : [];

        // if empty, don’t kill it. just slow it down a bit.
        if (items.length === 0) {
          const next = minutesFromNow(Math.max(feed.poll_interval_minutes ?? DEFAULT_POLL_MINUTES, 60));
          await supabase.from("feeds").update({
            last_polled_at: new Date().toISOString(),
            next_poll_at: next,
            status: "degraded"
          }).eq("id", feed.id);

          log.processed += 1;
          continue;
        }

        // upsert items by canonical_hash (your new DB contract)
        const rows = items.map((it: any) => {
          const urlRaw = it?.link || it?.url || "";
          const urlNorm = normalizeUrl(urlRaw);
          const guid = it?.guid || it?.id || null;
          const canonical_hash = itemCanonicalHash(feed.id, urlNorm, guid);

          const published = it?.isoDate ? new Date(it.isoDate).toISOString() : (it?.pubDate ? new Date(it.pubDate).toISOString() : null);
          const published_at = published || new Date().toISOString();

          return {
            feed_id: feed.id,
            title: it?.title ?? "(untitled)",
            url: urlRaw,
            url_normalized: urlNorm,
            canonical_hash,
            source_guid: guid,
            author: it?.creator || it?.author || null,
            summary: it?.contentSnippet || it?.summary || it?.content || null,
            image_url: findImage(it),
            published_at,
            published_at_corrected: published_at,
            title_normalized: normalizeTitle(it?.title ?? null)
          };
        });

        // Supabase upsert
        const { error: upErr } = await supabase
          .from("items")
          .upsert(rows, { onConflict: "canonical_hash" });

        if (upErr) throw upErr;

        // feed success + schedule next poll
        const pollMins = feed.poll_interval_minutes ?? DEFAULT_POLL_MINUTES;
        await supabase.from("feeds").update({
          last_fetched_at: new Date().toISOString(),
          last_success_at: new Date().toISOString(),
          last_polled_at: new Date().toISOString(),
          consecutive_error_count: 0,
          status: "active",
          next_poll_at: minutesFromNow(pollMins)
        }).eq("id", feed.id);

        log.processed += 1;
      } catch (err: any) {
        log.errors += 1;

        const fatal = isFatalError(err);
        const nextErrCount = (feed.consecutive_error_count ?? 0) + 1;

        // log error row (feed_id is now NOT NULL)
        await supabase.from("feed_errors").insert({
          feed_id: feed.id,
          error_code: fatal ? "fatal" : "transient",
          error_message: err?.message || String(err),
          occurred_at: new Date().toISOString()
        });

        // backoff
        const base = feed.poll_interval_minutes ?? DEFAULT_POLL_MINUTES;
        const backoffMins = fatal ? 24 * 60 : Math.min(360, base * Math.pow(2, Math.min(nextErrCount, 5)));
        const status = fatal || nextErrCount >= 10 ? "broken" : "degraded";

        await supabase.from("feeds").update({
          consecutive_error_count: nextErrCount,
          last_error_at: new Date().toISOString(),
          last_error_code: fatal ? "fatal" : "transient",
          status,
          // only hard-disable on truly fatal or long streaks
          is_active: status === "broken" ? false : true,
          next_poll_at: minutesFromNow(backoffMins)
        }).eq("id", feed.id);
      }
    }

    // retention cleanup (lightweight)
    await supabase.rpc("delete_old_items", { days: RETENTION_DAYS }).catch(() => null);

    res.json({ ...log, finished: nowIso() });
  } catch (e: any) {
    console.error("ingest job failed", e);
    res.status(500).json({ ok: false, error: e?.message || String(e), started, finished: nowIso() });
  }
});

function minutesFromNow(minutes: number) {
  return new Date(Date.now() + minutes * 60_000).toISOString();
}

app.listen(Number(PORT), () => {
  console.log(`feeds-worker listening on :${PORT}`);
});

