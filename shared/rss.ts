import Parser from "rss-parser";

export type ParsedFeed = {
  title?: string;
  link?: string;
  items?: any[];
};

export const parser = new Parser({
  timeout: 5000,
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    Accept: "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
  },
  customFields: {
    item: [
      ["media:content", "mediaContent"],
      ["content:encoded", "contentEncoded"],
    ],
  },
});

export const findImage = (item: any): string | null => {
  if (item?.enclosure?.url) return item.enclosure.url;

  if (item?.mediaContent) {
    if (Array.isArray(item.mediaContent)) return item.mediaContent[0]?.$?.url || null;
    if (item.mediaContent?.$?.url) return item.mediaContent.$.url;
  }

  if (item?.itunes?.image) return item.itunes.image;

  const content = item?.contentEncoded || item?.content || item?.description || "";
  const imgMatch = String(content).match(/<img[^>]+src="([^">]+)"/);
  if (imgMatch?.[1]) return imgMatch[1];

  return null;
};

export const isFatalError = (err: any) => {
  const msg = (err?.message || String(err)).toLowerCase();
  return (
    msg.includes("status code 404") ||
    msg.includes("status code 403") ||
    msg.includes("status code 410") ||
    msg.includes("non-whitespace before first tag") ||
    msg.includes("unexpected close tag") ||
    msg.includes("invalid character")
  );
};

