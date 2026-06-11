import "dotenv/config";
import express from "express";
import cors from "cors";
import fs from "fs";

const log = (msg) => {
    fs.appendFileSync("server.log", `${new Date().toISOString()} - ${msg}\n`);
};

const app = express();

app.use(cors({
    origin: [
        "http://localhost:5173",
        "http://127.0.0.1:5173"
    ],
}));
app.use(express.json());

app.post("/chat", async (req, res) => {
    const { message } = req.body;
    log(`Received /chat request with message: "${message}"`);

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();

    const abortController = new AbortController();
    res.on("close", () => {
        if (!res.writableEnded) {
            log("Client closed connection prematurely. Aborting Grok API request.");
            abortController.abort();
        }
    });

    try {
        if (!process.env.XAI_API_KEY || process.env.XAI_API_KEY === "your_xai_api_key_here") {
            log("API Key error: Key not found or placeholder used.");
            res.write("Error: Please configure a valid XAI_API_KEY in the backend .env file.");
            res.end();
            return;
        }

        const response = await fetch("https://api.x.ai/v1/responses", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${process.env.XAI_API_KEY}`
            },
            body: JSON.stringify({
                model: process.env.GROK_MODEL || "grok-beta",
                input: [
                    { role: "user", content: message }
                ],
                stream: true
            }),
            signal: abortController.signal
        });

        if (!response.ok) {
            const errorText = await response.text();
            log(`Grok API error status: ${response.status}. Body: ${errorText}`);
            res.write(`Grok API Error (${response.status}): ${errorText}`);
            res.end();
            return;
        }
        log("Grok API response OK, starting to read stream...");

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop(); // Hold onto any incomplete lines

            for (const line of lines) {
                const cleanedLine = line.trim();
                if (!cleanedLine) continue;
                if (cleanedLine === "data: [DONE]") break;

                if (cleanedLine.startsWith("data: ")) {
                    try {
                        const parsedData = JSON.parse(cleanedLine.slice(6));
                        
                        // Handle text delta for Responses API
                        if (parsedData.type === "response.output_text.delta" && parsedData.delta) {
                            res.write(`data: ${JSON.stringify({ text: parsedData.delta })}\n\n`);
                        }

                        // Handle errors in stream
                        if (parsedData.type === "error" && parsedData.error) {
                            res.write(`data: ${JSON.stringify({ error: parsedData.error.message })}\n\n`);
                        }
                        if (parsedData.type === "response.failed" && parsedData.response?.error) {
                            res.write(`data: ${JSON.stringify({ error: parsedData.response.error.message })}\n\n`);
                        }
                    } catch (e) {
                        console.error("JSON parse warning: ", e.message, cleanedLine);
                    }
                }
            }
        }

        // Parse remaining buffer if any
        if (buffer.trim()) {
            const cleanedLine = buffer.trim();
            if (cleanedLine.startsWith("data: ") && cleanedLine !== "data: [DONE]") {
                try {
                    const parsedData = JSON.parse(cleanedLine.slice(6));
                    if (parsedData.type === "response.output_text.delta" && parsedData.delta) {
                        res.write(`data: ${JSON.stringify({ text: parsedData.delta })}\n\n`);
                    }
                } catch (e) {
                    console.error("JSON final parse warning: ", e.message, cleanedLine);
                }
            }
        }
    } catch (err) {
        if (err.name === "AbortError") {
            log("Grok API request aborted by client.");
        } else {
            log(`Error fetching from Grok API: ${err.message}`);
            res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        }
    } finally {
        log("Response stream finished.");
        res.write("data: [DONE]\n\n");
        res.end();
    }
});

app.listen(5001, () => {
    log("Server listening on port 5001");
    log(`XAI_API_KEY loaded on startup: ${process.env.XAI_API_KEY ? "Yes" : "No"}`);
});
