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

// News sentiment palette (tweak)
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

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

function alignedWindowEndUtc(date: Date = new Date()): string {
  const ms = date.getTime();
  const fiveMin = 5 * 60 * 1000;
  const aligned = new Date(Math.floor(ms / fiveMin) * fiveMin);
  return aligned.toISOString();
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
  const norm = words.map((w: string) => w.trim().toUpperCase()).join("|");
  return crypto.createHash("sha256").update(norm).digest("hex");
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

export async function recomputeOrbs(req: Request, res: Response) {
  const window_end = alignedWindowEndUtc(new Date());
  const window_minutes = Number(req.body?.window_minutes ?? 60);

  // 1) Ensure item_categories is incrementally populated (last 2 hours)
  await supabase.rpc("fn_item_categories_backfill_recent", { p_hours: 2 });

  // 2) Load enabled topics
  const { data: topicsRaw, error: topicsErr } = await supabase
    .from("topics")
    .select("id,name,slug,orb_color,cadence_minutes,is_enabled,uses_sentiment_color")
    .eq("is_enabled", true)
    .order("sort_order", { ascending: true });

  if (topicsErr || !topicsRaw) {
    return res.status(500).json({ ok: false, error: topicsErr?.message ?? "topics load failed" });
  }

  const topics = topicsRaw as TopicRow[];
  const results: Array<Record<string, unknown>> = [];

  for (const t of topics) {
    const topic_id = t.id;
    const wantsSentiment = Boolean(t.uses_sentiment_color);

    // 3) Create run row (per topic)
    const { data: runRow, error: runErr } = await supabase
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
      .select()
      .single();

    if (runErr || !runRow) {
      results.push({ topic_id, ok: false, stage: "run_insert", error: runErr?.message });
      continue;
    }
    const run_id = runRow.id as string;

    try {
      // 4) State calc (RPC)
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

      // 5) Upsert orb_state
      await supabase
        .from("orb_state")
        .upsert(
          {
            topic_id,
            window_end,
            window_minutes,
            volume,
            velocity: 0,
            diversity,
            top_sources,
            top_items,
            input_hash: state_hash,
            computed_at: new Date().toISOString(),
            run_id,
          },
          { onConflict: "topic_id,window_end,window_minutes" }
        );

      // Velocity vs previous window
      const prev_end = new Date(new Date(window_end).getTime() - window_minutes * 60 * 1000).toISOString();
      const { data: prevState } = await supabase
        .from("orb_state")
        .select("volume")
        .eq("topic_id", topic_id)
        .eq("window_end", prev_end)
        .eq("window_minutes", window_minutes)
        .maybeSingle();

      const prevVol = Number((prevState as { volume?: number } | null)?.volume ?? 0);
      const velocity = prevVol > 0 ? (volume - prevVol) / Math.max(prevVol, 1) : 0;

      await supabase
        .from("orb_state")
        .update({ velocity })
        .eq("topic_id", topic_id)
        .eq("window_end", window_end)
        .eq("window_minutes", window_minutes);

      // 6) Determine whether to regen label
      const { data: snap } = await supabase
        .from("orb_snapshots")
        .select("updated_at, output_hash, keywords")
        .eq("topic_id", topic_id)
        .maybeSingle();

      const cadenceMin = Number(t.cadence_minutes ?? 30);
      const lastUpdatedMs = (snap as { updated_at?: string } | null)?.updated_at
        ? new Date((snap as { updated_at: string }).updated_at).getTime()
        : 0;

      const timeGateOk = !lastUpdatedMs || Date.now() - lastUpdatedMs >= cadenceMin * 60 * 1000;

      const changeGateOk =
        Math.abs(velocity) >= 0.35 ||
        (volume >= 30 && prevVol > 0 && Math.abs(volume - prevVol) / Math.max(prevVol, 1) >= 0.25);

      const minVolOk = volume >= 12;

      let didLabel = false;
      let label_status: "stale" | "candidate" | "promoted" = "stale";
      let promotedWords: string[] | null = null;
      let sentiment_label: string | null = null;
      let output_hash: string | null = (snap as { output_hash?: string } | null)?.output_hash ?? null;

      if (timeGateOk && changeGateOk && minVolOk) {
        // 7) Candidate selection
        const { data: candRowsRaw, error: candErr } = await supabase.rpc("fn_orb_label_candidates", {
          p_topic_id: topic_id,
          p_window_end: window_end,
          p_window_minutes: window_minutes,
          p_max_items: 15,
          p_max_per_feed: 2,
        });

        const candRows = (candRowsRaw ?? []) as LabelCandidatesRow[];

        if (!candErr && candRows[0]) {
          const cand = candRows[0];
          const items = (cand.items ?? []) as LabelCandidateItem[];
          const label_input_hash = cand.label_input_hash;

          if (items.length >= 8) {
            // 8) OpenAI call
            const ai = await callOpenAIThreeWords({
              topicName: t.name,
              topicSlug: t.slug,
              items,
              wantsSentiment,
            });

            const words = ai.words;
            const outHash = hashWords(words);

            // 9) Insert candidate label
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

            if (!ins.error) {
              didLabel = true;

              // 10) Promotion: two consecutive same hashes
              const { data: lastTwo } = await supabase
                .from("orb_labels")
                .select("id,output_hash")
                .eq("topic_id", topic_id)
                .order("generated_at", { ascending: false })
                .limit(2);

              const last = (lastTwo ?? []) as Array<{ id: string; output_hash: string }>;

              if (last.length === 2 && last[0].output_hash === last[1].output_hash) {
                await supabase.from("orb_labels").update({ status: "promoted" }).eq("id", last[0].id);
                await supabase.from("orb_labels").update({ status: "rejected" }).eq("id", last[1].id);

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

      // 11) Load latest promoted label if none promoted this run
      if (!promotedWords) {
        const { data: prom } = await supabase
          .from("orb_labels")
          .select("words,sentiment_label,output_hash")
          .eq("topic_id", topic_id)
          .eq("status", "promoted")
          .order("generated_at", { ascending: false })
          .limit(1)
          .maybeSingle();

        const p = prom as { words?: string[]; sentiment_label?: string | null; output_hash?: string | null } | null;
        if (p?.words?.length === 3) {
          promotedWords = p.words;
          sentiment_label = p.sentiment_label ?? null;
          output_hash = p.output_hash ?? output_hash;
          label_status = "promoted";
        }
      }

      const resting_color = t.orb_color ?? "#999999";
      const display_color = wantsSentiment
        ? sentiment_label
          ? SENTIMENT_COLORS[sentiment_label.toLowerCase()] ?? resting_color
          : resting_color
        : resting_color;

      // 12) Upsert snapshot for UI
      await supabase
        .from("orb_snapshots")
        .upsert(
          {
            topic_id,
            keywords: promotedWords ?? ((snap as { keywords?: string[] } | null)?.keywords ?? []),
            sentiment_label,
            sentiment_score: null,
            summary: null,
            output_hash,
            updated_at: new Date().toISOString(),
            window_end,
            window_minutes,
            volume,
            velocity,
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

      // 13) Finish run
      await supabase
        .from("orb_runs")
        .update({
          status: "ok",
          output_hash,
          token_estimate: null,
          finished_at: new Date().toISOString(),
        })
        .eq("id", run_id);

      results.push({ topic_id, ok: true, volume, diversity, velocity, didLabel, label_status });
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
