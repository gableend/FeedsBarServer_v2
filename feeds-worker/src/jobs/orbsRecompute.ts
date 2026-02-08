import type { Request, Response } from "express";
import crypto from "crypto";
import OpenAI from "openai";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}
if (!OPENAI_API_KEY) {
  throw new Error("Missing OPENAI_API_KEY");
}

const PROMPT_VERSION = "orbs-words-v1";
const MODEL = process.env.ORBS_OPENAI_MODEL || "gpt-4o-mini";

// UI safety cap for orb motion (per-hour velocity)
const VELOCITY_UI_CAP = 5;

const SENTIMENT_COLORS: Record<string, string> = {
  red: "#E24D4D",
  amber: "#F2B233",
  green: "#3CCB7F",
};

type TopicRow = {
  id: string;
  name: string;
  slug: string;
  orb_color: string | null;
  cadence_minutes: number | null;
  is_enabled: boolean;
  uses_sentiment_color: boolean | null;
};

type OrbStateCalcRow = {
  volume: number;
  diversity: number;
  top_sources: unknown[];
  top_items: unknown[];
  input_hash: string;
};

type LabelCandidateItem = {
  id: string;
  title: string;
  feed_id: string | null;
  published_at: string;
};

type LabelCandidatesRow = {
  items: LabelCandidateItem[];
  label_input_hash: string;
};

type OrbRunRow = { id: string };

type OrbSnapshotRow = {
  output_hash: string | null;
  keywords: string[] | null;
};

type OrbLabelRow = {
  id: string;
  output_hash: string | null;
  words: string[] | null;
  sentiment_label: string | null;
  generated_at: string;
  status: "candidate" | "promoted" | "rejected" | "stale";
};

type PrevStateRow = {
  volume: number | null;
  window_end: string;
};

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

function alignedWindowEndUtc(date: Date = new Date()): string {
  // align to 5-min grid
  const ms = date.getTime();
  const step = 5 * 60 * 1000;
  const aligned = new Date(Math.floor(ms / step) * step);
  return aligned.toISOString();
}

function clampWindowMinutes(n: number): number {
  if (!Number.isFinite(n) || n <= 0) return 60;
  if (n < 5) return 5;
  if (n > 24 * 60) return 24 * 60;
  return Math.round(n);
}

function normalizeTitle(t: string): string {
  return t.replace(/\s+/g, " ").trim();
}

function safeThreeWords(raw: string): string[] {
  const cleaned = raw.replace(/\s+/g, " ").trim();
  const parts = cleaned
    .split(/[,|/]/g)
    .map((s: string) => s.trim())
    .filter((s: string) => Boolean(s));

  const words = (parts.length >= 3 ? parts.slice(0, 3) : parts)
    .map((w: string) => w.replace(/[^a-zA-Z0-9\- ]/g, "").trim())
    .filter((w: string) => Boolean(w))
    .slice(0, 3);

  while (words.length < 3) words.push("…");
  return words.map((w: string) => w.toUpperCase());
}

function hashWords(words: string[]): string {
  const norm = words.map((w) => w.trim().toUpperCase()).join("|");
  return crypto.createHash("sha256").update(norm).digest("hex");
}

function toIsoOrNull(x: unknown): string | null {
  if (typeof x !== "string") return null;
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function minutesBetweenIso(prevIso: string, curIso: string): number {
  const a = new Date(prevIso).getTime();
  const b = new Date(curIso).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  const diffMin = (b - a) / 60000;
  return diffMin;
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function callOpenAIThreeWords(args: {
  topicName: string;
  topicSlug: string;
  items: Array<{ title: string; published_at: string; feed_id: string | null }>;
  wantsSentiment: boolean;
}): Promise<{
  words: string[];
  sentiment_label: string | null;
  raw: string;
  token_estimate: number | null;
}> {
  const { topicName, topicSlug, items, wantsSentiment } = args;

  const bulletList = items
    .map((it) => `- ${normalizeTitle(it.title)} (${it.published_at})`)
    .join("\n");

  const system = `You generate a concise 3-word headline for a topic based on recent RSS item titles.
Rules:
- Output EXACTLY three words in ALL CAPS, separated by commas.
- No extra text.
- Prefer concrete entities/themes over generic words.
- Avoid profanity.
${wantsSentiment ? "- Also choose a sentiment color: red, amber, or green." : ""}`;

  const user = `Topic: ${topicName} (${topicSlug})
Recent items:
${bulletList}

Return:
Line 1: WORD, WORD, WORD
${wantsSentiment ? "Line 2: SENTIMENT: red|amber|green" : ""}`;

  const resp = await openai.chat.completions.create({
    model: MODEL,
    temperature: 0.2,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
  });

  const text = resp.choices?.[0]?.message?.content?.trim() ?? "";
  const lines = text
    .split("\n")
    .map((l: string) => l.trim())
    .filter((l: string) => Boolean(l));

  const wordsLine = lines[0] ?? "";
  const words = safeThreeWords(wordsLine);

  let sentiment_label: string | null = null;

  if (wantsSentiment) {
    const sentLine = lines.find((l: string) => l.toLowerCase().startsWith("sentiment:"));
    if (sentLine) {
      const val = sentLine.split(":")[1]?.trim().toLowerCase();
      if (val && ["red", "amber", "green"].includes(val)) sentiment_label = val;
    }
  }

  return {
    words,
    sentiment_label,
    raw: text,
    token_estimate: resp.usage?.total_tokens ?? null,
  };
}

type TopicResult = {
  topic_id: string;
  ok: boolean;

  volume?: number;
  diversity?: number;

  // velocity output for debugging + UI understanding
  velocity?: number; // raw percent delta
  velocity_per_hour?: number;
  velocity_snapshot?: number; // capped per-hour used in snapshot
  elapsed_minutes?: number;

  prev_window_end?: string | null;
  prev_volume?: number;

  didLabel?: boolean;
  label_status?: "stale" | "candidate" | "promoted";

  // Debug / observability
  wantsSentiment?: boolean;
  window_end?: string;
  window_minutes?: number;

  cadence_minutes?: number;
  last_label_attempt_at?: string | null;
  timeGateOk?: boolean;

  changeGateOk?: boolean;
  minVolOk?: boolean;
  firstLabelOk?: boolean;
  regenOk?: boolean;

  label_attempted?: boolean;
  cand_count?: number | null;
  cand_error?: string | null;
  openai_error?: string | null;
  openai_retry_attempted?: boolean;
  openai_retry_succeeded?: boolean;
  label_insert_error?: string | null;

  stage?: string;
  error?: string;
};

export async function recomputeOrbs(req: Request, res: Response) {
  const window_end = alignedWindowEndUtc(new Date());
  const window_minutes = clampWindowMinutes(Number(req.body?.window_minutes ?? 60));

  // 1) Backfill categories for recent items
  {
    const { error } = await supabase.rpc("fn_item_categories_backfill_recent", { p_hours: 2 });
    if (error) {
      return res.status(500).json({
        ok: false,
        error: "fn_item_categories_backfill_recent failed",
        details: error.message,
      });
    }
  }

  // 2) Enabled topics
  const { data: topicsRaw, error: topicsErr } = await supabase
    .from("topics")
    .select("id,name,slug,orb_color,cadence_minutes,is_enabled,uses_sentiment_color")
    .eq("is_enabled", true)
    .order("sort_order", { ascending: true });

  if (topicsErr || !topicsRaw) {
    return res.status(500).json({ ok: false, error: topicsErr?.message ?? "topics load failed" });
  }

  const topics = topicsRaw as TopicRow[];
  const results: TopicResult[] = [];

  for (const t of topics) {
    const topic_id = t.id;

    // Sentiment: prefer column, but fall back to slug for now
    const wantsSentiment = Boolean(t.uses_sentiment_color) || t.slug === "news-sentiment";

    // 3) Create run row
    const { data: runRowRaw, error: runErr } = await supabase
      .from("orb_runs")
      .insert({
        topic_id,
        status: "started",
        prompt_version: PROMPT_VERSION,
        started_at: new Date().toISOString(),
        window_end,
        window_minutes,
        model: MODEL,
      })
      .select("id")
      .single();

    if (runErr || !runRowRaw) {
      results.push({ topic_id, ok: false, stage: "run_insert", error: runErr?.message ?? "run insert failed" });
      continue;
    }

    const run_id = (runRowRaw as OrbRunRow).id;

    try {
      // 4) Calc state (RPC)
      const { data: stateRowsRaw, error: stateErr } = await supabase.rpc("fn_orb_state_calc", {
        p_topic_id: topic_id,
        p_window_end: window_end,
        p_window_minutes: window_minutes,
      });

      const stateRows = (stateRowsRaw ?? []) as OrbStateCalcRow[];
      if (stateErr || !stateRows[0]) throw new Error(stateErr?.message ?? "state calc failed");

      const state = stateRows[0];
      const volume = Number(state.volume ?? 0);
      const diversity = Number(state.diversity ?? 0);
      const top_sources = state.top_sources ?? [];
      const top_items = state.top_items ?? [];
      const state_hash = state.input_hash;

      // 5) Find previous actual orb_state row (most recent prior window_end)
      const { data: prevStateRaw, error: prevErr } = await supabase
        .from("orb_state")
        .select("volume, window_end")
        .eq("topic_id", topic_id)
        .eq("window_minutes", window_minutes)
        .lt("window_end", window_end)
        .order("window_end", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (prevErr) throw new Error(`prev state lookup failed: ${prevErr.message}`);

      const prevState = (prevStateRaw ?? null) as PrevStateRow | null;
      const prevVol = Number(prevState?.volume ?? 0);
      const prev_window_end = toIsoOrNull(prevState?.window_end ?? null);

      const elapsed_minutes =
        prev_window_end !== null ? minutesBetweenIso(prev_window_end, window_end) : 0;

      // Raw velocity (percent change vs prevVol)
      const velocity_raw = prevVol > 0 ? (volume - prevVol) / prevVol : 0;

      // Time-normalized velocity (per hour)
      const denom = elapsed_minutes > 0 ? elapsed_minutes : 0;
      const velocity_per_hour = denom > 0 ? velocity_raw * (60 / denom) : 0;

      // UI velocity (capped per-hour) for snapshots
      const velocity_snapshot = clamp(velocity_per_hour, -VELOCITY_UI_CAP, VELOCITY_UI_CAP);

      // 6) Upsert orb_state (single write, includes both velocities)
      {
        const { error } = await supabase.from("orb_state").upsert(
          {
            topic_id,
            window_end,
            window_minutes,
            volume,
            velocity: velocity_raw,
            velocity_per_hour,
            diversity,
            top_sources,
            top_items,
            input_hash: state_hash,
            computed_at: new Date().toISOString(),
            run_id,
          },
          { onConflict: "topic_id,window_end,window_minutes" }
        );

        if (error) throw new Error(`orb_state upsert failed: ${error.message}`);
      }

      // 7) Snapshot load
      const { data: snapRaw, error: snapErr } = await supabase
        .from("orb_snapshots")
        .select("output_hash, keywords")
        .eq("topic_id", topic_id)
        .maybeSingle();

      if (snapErr) throw new Error(`snapshot load failed: ${snapErr.message}`);
      const snap = (snapRaw ?? null) as OrbSnapshotRow | null;

      // 8) Label cadence clock uses orb_labels.generated_at
      const { data: lastLabelAttemptRaw, error: lastLabelErr } = await supabase
        .from("orb_labels")
        .select("generated_at")
        .eq("topic_id", topic_id)
        .order("generated_at", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (lastLabelErr) throw new Error(`last label attempt lookup failed: ${lastLabelErr.message}`);

      const cadenceMin = Number(t.cadence_minutes ?? 30);
      const lastAttemptIso = lastLabelAttemptRaw?.generated_at
        ? new Date(lastLabelAttemptRaw.generated_at).toISOString()
        : null;

      const lastAttemptMs = lastLabelAttemptRaw?.generated_at
        ? new Date(lastLabelAttemptRaw.generated_at).getTime()
        : 0;

      const timeGateOk = !lastAttemptMs || Date.now() - lastAttemptMs >= cadenceMin * 60 * 1000;

      // Change gate uses RAW velocity (keeps previous behavior stable)
      const absVel = Math.abs(velocity_raw);

      const changeGateOk =
        absVel >= 0.35 ||
        (volume >= 30 && prevVol > 0 && Math.abs(volume - prevVol) / prevVol >= 0.25);

      const minVolOk = volume >= 12;

      // 9) Find latest promoted label (if any)
      const { data: promotedExistingRaw, error: promotedErr } = await supabase
        .from("orb_labels")
        .select("id, output_hash, words, sentiment_label, generated_at, status")
        .eq("topic_id", topic_id)
        .eq("status", "promoted")
        .order("generated_at", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (promotedErr) throw new Error(`promoted label lookup failed: ${promotedErr.message}`);

      const promotedExisting = (promotedExistingRaw ?? null) as OrbLabelRow | null;
      const hasPromoted = Boolean(promotedExisting?.id);

      // First label promotes immediately if no promoted exists yet
      const firstLabelOk = !hasPromoted && timeGateOk && volume >= 10;
      const regenOk = hasPromoted && timeGateOk && changeGateOk && minVolOk;

      let didLabel = false;
      let label_status: "stale" | "candidate" | "promoted" = "stale";
      let promotedWords: string[] | null = null;
      let sentiment_label: string | null = null;
      let output_hash: string | null = snap?.output_hash ?? null;

      // Debug labeling counters
      let label_attempted = false;
      let cand_count: number | null = null;
      let cand_error: string | null = null;
      let openai_error: string | null = null;
      let openai_retry_attempted = false;
      let openai_retry_succeeded = false;
      let run_token_estimate: number | null = null;
      let label_insert_error: string | null = null;

      // 10) Labeling attempt
      if (firstLabelOk || regenOk) {
        label_attempted = true;

        const { data: candRowsRaw, error: candErr } = await supabase.rpc("fn_orb_label_candidates", {
          p_topic_id: topic_id,
          p_window_end: window_end,
          p_window_minutes: window_minutes,
          p_max_items: 15,
          p_max_per_feed: hasPromoted ? 2 : 3,
        });

        cand_error = candErr?.message ?? null;

        const candRows = (candRowsRaw ?? []) as LabelCandidatesRow[];
        const cand = candRows[0] ?? null;

        if (!candErr && cand) {
          const items = (cand.items ?? []) as LabelCandidateItem[];
          cand_count = items.length;

          const label_input_hash = cand.label_input_hash;
          const minItems = hasPromoted ? 8 : 6;

          if (items.length >= minItems) {
            // OpenAI call (retry once with small backoff)
            let ai: Awaited<ReturnType<typeof callOpenAIThreeWords>> | null = null;
            try {
              ai = await callOpenAIThreeWords({
                topicName: t.name,
                topicSlug: t.slug,
                items,
                wantsSentiment,
              });
            } catch (e1: unknown) {
              openai_error = e1 instanceof Error ? e1.message : String(e1);
              openai_retry_attempted = true;
              await sleep(600);
              try {
                ai = await callOpenAIThreeWords({
                  topicName: t.name,
                  topicSlug: t.slug,
                  items,
                  wantsSentiment,
                });
                openai_retry_succeeded = true;
                openai_error = null;
              } catch (e2: unknown) {
                const msg2 = e2 instanceof Error ? e2.message : String(e2);
                openai_error = `${openai_error}; retry_failed: ${msg2}`;
              }
            }

            if (ai) {
              run_token_estimate = ai.token_estimate;
              const words = ai.words;
              const outHash = hashWords(words);

              const ins = await supabase.from("orb_labels").insert({
                topic_id,
                window_end,
                window_minutes,
                words,
                summary: null,
                sentiment_label: ai.sentiment_label,
                sentiment_score: null,
                input_hash: label_input_hash,
                output_hash: outHash,
                model: MODEL,
                prompt_version: PROMPT_VERSION,
                status: "candidate",
                run_id,
              });

              label_insert_error = ins.error?.message ?? null;

              if (!ins.error) {
                didLabel = true;

                if (!hasPromoted) {
                  // Bootstrap: promote immediately
                  const { data: newestRaw, error: newestErr } = await supabase
                    .from("orb_labels")
                    .select("id")
                    .eq("topic_id", topic_id)
                    .order("generated_at", { ascending: false })
                    .limit(1)
                    .maybeSingle();

                  if (newestErr) throw new Error(`newest label lookup failed: ${newestErr.message}`);

                  const newestId = (newestRaw as { id?: string } | null)?.id ?? null;
                  if (newestId) {
                    const { error: promoteErr2 } = await supabase
                      .from("orb_labels")
                      .update({ status: "promoted" })
                      .eq("id", newestId);

                    if (promoteErr2) throw new Error(`bootstrap promote failed: ${promoteErr2.message}`);
                  }

                  promotedWords = words;
                  sentiment_label = ai.sentiment_label;
                  output_hash = outHash;
                  label_status = "promoted";
                } else {
                  // Stability rule: promote after two consecutive same hashes
                  const { data: lastTwoRaw, error: lastTwoErr } = await supabase
                    .from("orb_labels")
                    .select("id,output_hash")
                    .eq("topic_id", topic_id)
                    .order("generated_at", { ascending: false })
                    .limit(2);

                  if (lastTwoErr) throw new Error(`lastTwo label lookup failed: ${lastTwoErr.message}`);

                  const last = (lastTwoRaw ?? []) as Array<{ id: string; output_hash: string | null }>;
                  const h0 = last[0]?.output_hash ?? null;
                  const h1 = last[1]?.output_hash ?? null;

                  if (last.length === 2 && h0 && h1 && h0 === h1) {
                    const { error: promoteErr } = await supabase
                      .from("orb_labels")
                      .update({ status: "promoted" })
                      .eq("id", last[0].id);

                    if (promoteErr) throw new Error(`promote failed: ${promoteErr.message}`);

                    // If hashes match, the prior row was just superseded. Mark as stale (not rejected).
                    const { error: staleErr } = await supabase
                      .from("orb_labels")
                      .update({ status: "stale" })
                      .eq("id", last[1].id);

                    if (staleErr) throw new Error(`stale failed: ${staleErr.message}`);

                    promotedWords = words;
                    sentiment_label = ai.sentiment_label;
                    output_hash = outHash;
                    label_status = "promoted";
                  } else {
                    label_status = "candidate";
                  }
                }
              }
            }
          }
        }
      }

      // 11) Load promoted label if none promoted this run
      if (!promotedWords && promotedExisting?.words && promotedExisting.words.length === 3) {
        promotedWords = promotedExisting.words;
        sentiment_label = promotedExisting.sentiment_label ?? null;
        output_hash = promotedExisting.output_hash ?? output_hash;
        label_status = "promoted";
      }

      const resting_color = t.orb_color ?? "#999999";
      const display_color = wantsSentiment
        ? sentiment_label
          ? SENTIMENT_COLORS[sentiment_label.toLowerCase()] ?? resting_color
          : resting_color
        : resting_color;

      // 12) Upsert snapshot for UI
      {
        const keywordsFallback = Array.isArray(snap?.keywords) ? snap!.keywords! : [];
        const keywordsToWrite = promotedWords ?? keywordsFallback;

        const { error } = await supabase.from("orb_snapshots").upsert(
          {
            topic_id,
            keywords: keywordsToWrite,
            sentiment_label,
            sentiment_score: null,
            summary: null,
            output_hash,
            updated_at: new Date().toISOString(),
            window_end,
            window_minutes,
            volume,
            // IMPORTANT: snapshot gets UI-friendly capped per-hour velocity
            velocity: velocity_snapshot,
            diversity,
            top_sources,
            top_items,
            state_hash,
            resting_color,
            display_color,
            label_status,
          },
          { onConflict: "topic_id" }
        );

        if (error) throw new Error(`snapshot upsert failed: ${error.message}`);
      }

      // 13) Finish run
      {
        const { error } = await supabase
          .from("orb_runs")
          .update({
            status: "ok",
            output_hash,
            token_estimate: run_token_estimate,
            finished_at: new Date().toISOString(),
          })
          .eq("id", run_id);

        if (error) throw new Error(`orb_runs update failed: ${error.message}`);
      }

      results.push({
        topic_id,
        ok: true,
        window_end,
        window_minutes,
        wantsSentiment,

        volume,
        diversity,

        velocity: velocity_raw,
        velocity_per_hour,
        velocity_snapshot,
        elapsed_minutes,

        prev_window_end,
        prev_volume: prevVol,

        cadence_minutes: cadenceMin,
        last_label_attempt_at: lastAttemptIso,
        timeGateOk,

        changeGateOk,
        minVolOk,
        firstLabelOk,
        regenOk,

        didLabel,
        label_status,

        label_attempted,
        cand_count,
        cand_error,
        openai_error,
        openai_retry_attempted,
        openai_retry_succeeded,
        label_insert_error,
      });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);

      await supabase
        .from("orb_runs")
        .update({
          status: "error",
          error_message: msg,
          finished_at: new Date().toISOString(),
        })
        .eq("id", run_id);

      results.push({ topic_id, ok: false, error: msg });
    }
  }

  res.json({ ok: true, window_end, window_minutes, topics: topics.length, results });
}
