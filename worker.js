// DROP Farewell RSVP — Cloudflare Worker
// Receives name from the RSVP page, creates a row in Notion

const NOTION_DATABASE_ID = "a2dea78d-80c0-4e4a-a660-c1cc162f2779";

export default {
  async fetch(request, env) {
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    try {
      const { name } = await request.json();

      if (!name || !name.trim()) {
        return jsonResponse({ error: "Name is required" }, 400);
      }

      // Create page in Notion database
      const notionRes = await fetch("https://api.notion.com/v1/pages", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${env.NOTION_API_KEY}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          parent: { database_id: NOTION_DATABASE_ID },
          properties: {
            "Name": {
              title: [{ text: { content: name.trim() } }],
            },
            "Status": {
              select: { name: "Confirmed" },
            },
          },
        }),
      });

      if (!notionRes.ok) {
        const err = await notionRes.text();
        console.error("Notion error:", err);
        return jsonResponse({ error: "Failed to save RSVP" }, 500);
      }

      return jsonResponse({ success: true, name: name.trim() });

    } catch (err) {
      console.error("Worker error:", err);
      return jsonResponse({ error: "Something went wrong" }, 500);
    }
  },
};

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  });
}
