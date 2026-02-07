export async function getSmartIconUrl(siteUrl: string): Promise<string> {
  try {
    const urlObj = new URL(siteUrl);
    const domain = urlObj.hostname
      .replace("www.", "")
      .replace("feeds.", "")
      .replace("rss.", "")
      .replace("api.", "");

    const clearbitUrl = `https://logo.clearbit.com/${domain}?size=128`;
    try {
      const res = await fetch(clearbitUrl);
      if (res.status === 200) return clearbitUrl;
    } catch {}

    return `https://icons.duckduckgo.com/ip3/${domain}.ico`;
  } catch {
    return "";
  }
}
