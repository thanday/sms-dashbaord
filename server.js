const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { Pool } = require("pg");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");
const session = require("express-session"); // Added
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const upload = multer({ dest: "uploads/" });
const { exec } = require("child_process");

// --- MIDDLEWARE ---
app.use(express.json());
app.use(express.urlencoded({ extended: true })); // Added for login form
app.use(express.static("public"));
app.set("view engine", "ejs");

// --- SESSION CONFIGURATION ---
app.use(
  session({
    secret: "sstv-internal-secret-2026",
    resave: false,
    saveUninitialized: true,
    cookie: { maxAge: 24 * 60 * 60 * 1000 }, // 24 hours
  })
);

// --- AUTH MIDDLEWARE ---
const isAdmin = (req, res, next) => {
  if (req.session.authenticated) {
    next();
  } else {
    req.session.returnTo = req.originalUrl;
    res.redirect("/login");
  }
};

app.post("/login", (req, res) => {
  const { username, password } = req.body;
  if (username === "admin" && password === "sstv@2026") {
    req.session.authenticated = true;
    const redirectTo = req.session.returnTo || "/admin";
    delete req.session.returnTo;
    res.redirect(redirectTo);
  } else {
    res.render("login", { error: "Invalid Username or Password" });
  }
});

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "sms_stats",
  password: "Sun.Media@94.6", // Ensure this is exactly like your server.js
  port: 5432,
});

// --- PAGE ROUTES ---
// Dashboard is now Home
app.get("/", (req, res) => res.render("dashboard"));
app.get("/dashboard/all", (req, res) => res.render("dashboard"));

// Protected Admin/Management Page
app.get("/admin", isAdmin, (req, res) => res.render("index"));

app.get("/dashboard/program/:id", async (req, res) => {
  const result = await pool.query("SELECT name FROM programs WHERE id = $1", [
    req.params.id,
  ]);
  res.render("program-detail", {
    programName: result.rows[0].name,
    programId: req.params.id,
  });
});

// --- LOGIN ROUTES ---
app.get("/login", (req, res) => {
  res.render("login", { error: null });
});

app.post("/login", (req, res) => {
  const { username, password } = req.body;
  // Set your admin credentials here
  if (username === "admin" && password === "sstv@2026") {
    req.session.authenticated = true;
    res.redirect("/admin");
  } else {
    res.render("login", { error: "Invalid Username or Password" });
  }
});

app.get("/logout", (req, res) => {
  req.session.destroy();
  res.redirect("/");
});

// --- API: PROGRAMS ---

app.get("/api/last-sync", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT MAX(received_at) as last_sync FROM sms_logs"
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 1. Check if draw was already taken
app.get("/api/check-draw/:programId", isAdmin, async (req, res) => {
  const { date } = req.query;
  const { programId } = req.params;
  try {
    const result = await pool.query(
      "SELECT id FROM draw_winners WHERE program_id = $1 AND draw_date = $2 LIMIT 1",
      [programId, date]
    );
    res.json({ exists: result.rows.length > 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Clear draw if user wants to replace it
app.delete("/api/clear-draw/:programId", isAdmin, async (req, res) => {
  const { date } = req.query;
  const { programId } = req.params;
  try {
    await pool.query(
      "DELETE FROM draw_winners WHERE program_id = $1 AND draw_date = $2",
      [programId, date]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Add or Update this in server.js
app.get('/api/draw-numbers/:programId', isAdmin, async (req, res) => {
  const { programId } = req.params;
  const { date, keyword } = req.query; 

  try {
      // 1. Precise Time Slots matching your Export tool
      const timeSlots = {
          'AA': { start: '13:00:00', end: '13:59:59' },
          'BB': { start: '14:00:00', end: '14:59:59' },
          'CC': { start: '15:00:00', end: '15:59:59' },
          'DD': { start: '16:00:00', end: '16:59:59' },
          'EE': { start: '17:00:00', end: '17:59:59' },
          'FF': { start: '18:00:00', end: '18:59:59' },
          'GG': { start: '19:00:00', end: '19:59:59' },
          'HH': { start: '20:00:00', end: '20:59:59' },
          'JJ': { start: '21:00:00', end: '21:59:59' },
          'KK': { start: '22:00:00', end: '22:59:59' },
          'LL': { start: '23:00:00', end: '23:59:59' },
          'MM': { start: '00:00:00', end: '00:59:59', nextDay: true }
      };

      const slot = timeSlots[keyword.toUpperCase()];
      if (!slot) return res.status(400).json({ error: "Invalid Slot" });

      // 2. Handle the Midnight Crossover
      let searchDate = date;
      if (slot.nextDay) {
          const d = new Date(date);
          d.setDate(d.getDate() + 1);
          searchDate = d.toISOString().split('T')[0];
      }

      // 3. Database Query
      const result = await pool.query(
          `SELECT msisdn, message_content FROM sms_logs 
           WHERE keyword_id = (SELECT id FROM keywords WHERE name = $1 AND program_id = $2)
           AND received_at >= $3::timestamp + $4::interval
           AND received_at <= $3::timestamp + $5::interval`,
          [keyword.toUpperCase(), programId, searchDate, slot.start, slot.end]
      );

      // 4. The "Inaameh Filter" (Crucial for matching ZIP export)
      const filteredNumbers = result.rows.filter(row => {
          const msg = (row.message_content || "").toString().trim().toUpperCase();
          // Accept if it's the slot keyword OR the program master keyword
          return msg === keyword.toUpperCase() || msg === "SSTV";
      }).map(row => row.msisdn);

      res.json({ numbers: filteredNumbers });
  } catch (err) {
      res.status(500).json({ error: err.message });
  }
});

// Route to export winners as CSV
app.get("/api/export-winners", isAdmin, async (req, res) => {
  try {
    const result = await pool.query(`
          SELECT w.day_number as "Day", w.draw_date as "Date", 
                 w.keyword as "Slot", w.msisdn as "Phone Number",
                 p.name as "Program"
          FROM draw_winners w
          JOIN programs p ON w.program_id = p.id
          ORDER BY w.draw_date DESC, w.day_number DESC
      `);

    const rows = result.rows;
    if (rows.length === 0) return res.status(404).send("No winners to export.");

    // Define CSV Headers
    const headers = Object.keys(rows[0]).join(",") + "\n";

    // Map data to CSV rows
    const csvData = rows
      .map((row) =>
        Object.values(row)
          .map((val) => `"${val}"`)
          .join(",")
      )
      .join("\n");

    // Set Headers to force browser download
    res.setHeader("Content-Type", "text/csv");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename=SSTV_Winners_${
        new Date().toISOString().split("T")[0]
      }.csv`
    );

    res.status(200).send(headers + csvData);
  } catch (err) {
    res.status(500).send("Export failed: " + err.message);
  }
});

// Get Day Number Helper
const getDayNumber = (dateStr) => {
  const start = new Date("2026-02-18");
  const current = new Date(dateStr);
  return Math.floor((current - start) / (1000 * 60 * 60 * 24)) + 1;
};

// API to save a winner
app.post("/api/save-winner", isAdmin, async (req, res) => {
  const { msisdn, keyword, programId, date } = req.body;
  const dayNum = getDayNumber(date);
  try {
    await pool.query(
      "INSERT INTO draw_winners (program_id, msisdn, keyword, draw_date, day_number) VALUES ($1, $2, $3, $4, $5)",
      [programId, msisdn, keyword, date, dayNum]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// View for Winners Gallery
app.get("/dashboard/winners", async (req, res) => {
  const result = await pool.query(`
      SELECT w.*, p.name as program_name 
      FROM draw_winners w 
      JOIN programs p ON w.program_id = p.id 
      ORDER BY draw_date DESC, day_number DESC, keyword ASC
  `);
  res.render("winners-gallery", { winners: result.rows });
});

app.get("/dashboard/program/:id/draw", isAdmin, async (req, res) => {
  try {
    const result = await pool.query("SELECT name FROM programs WHERE id = $1", [
      req.params.id,
    ]);
    if (result.rows.length === 0) {
      return res.status(404).send("Program not found");
    }
    res.render("draw-room", {
      programName: result.rows[0].name,
      programId: req.params.id,
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

app.put("/api/keywords/:id", async (req, res) => {
  const { id } = req.params;
  const { name, active_time } = req.body;
  try {
    await pool.query(
      "UPDATE keywords SET name = $1, active_time = $2 WHERE id = $3",
      [name, active_time, id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/sync-ooredoo", (req, res) => {
  const child = exec("node scraper.js ooredoo");
  child.stdout.on("data", (data) => console.log(`Ooredoo: ${data}`));
  child.stderr.on("data", (data) => console.error(`Ooredoo Error: ${data}`));
  child.on("close", () => io.emit("dataUpdated"));
  res.json({ success: true });
});

app.post("/api/sync-dhiraagu", (req, res) => {
  const child = exec("node scraper.js dhiraagu");
  child.stdout.on("data", (data) => console.log(`Dhiraagu: ${data}`));
  child.stderr.on("data", (data) => console.error(`Dhiraagu Error: ${data}`));
  child.on("close", () => io.emit("dataUpdated"));
  res.json({ success: true });
});

app.get("/api/programs", async (req, res) => {
  const result = await pool.query("SELECT * FROM programs ORDER BY name ASC");
  res.json(result.rows);
});

app.post("/api/programs", async (req, res) => {
  const { name } = req.body;
  await pool.query("INSERT INTO programs (name) VALUES ($1)", [name]);
  res.json({ success: true });
});

app.get("/api/top-spenders/:programId", async (req, res) => {
  try {
    const { programId } = req.params;
    const query = `
            SELECT 
                s.msisdn, 
                COUNT(*) as total_messages,
                MAX(s.operator) as operator 
            FROM sms_logs s
            JOIN keywords k ON s.keyword_id = k.id
            WHERE k.program_id = $1
            GROUP BY s.msisdn
            ORDER BY total_messages DESC
            LIMIT 10;
        `;
    const result = await pool.query(query, [programId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const archiver = require("archiver");

app.get("/api/download-inaameh-zip", async (req, res) => {
  const { programId, date } = req.query;

  try {
    const timeSlots = {
      AA: { start: "13:00:00", end: "13:59:59" },
      BB: { start: "14:00:00", end: "14:59:59" },
      CC: { start: "15:00:00", end: "15:59:59" },
      DD: { start: "16:00:00", end: "16:59:59" },
      EE: { start: "17:00:00", end: "17:59:59" },
      FF: { start: "18:00:00", end: "18:59:59" },
      GG: { start: "19:00:00", end: "19:59:59" },
      HH: { start: "20:00:00", end: "20:59:59" },
      JJ: { start: "21:00:00", end: "21:59:59" },
      KK: { start: "22:00:00", end: "22:59:59" },
      LL: { start: "23:00:00", end: "23:59:59" },
      MM: { start: "00:00:00", end: "00:59:59", nextDay: true },
    };

    const keywordsResult = await pool.query(
      "SELECT id, name FROM keywords WHERE program_id = $1",
      [programId]
    );
    const keywords = keywordsResult.rows;

    const archive = archiver("zip", { zlib: { level: 9 } });
    res.attachment(`SSTV_Inaameh_Slots_${date}.zip`);
    archive.pipe(res);

    for (const kw of keywords) {
      const slot = timeSlots[kw.name.toUpperCase()];
      if (!slot) continue;

      let searchDate = date;
      if (slot.nextDay) {
        const d = new Date(date);
        d.setDate(d.getDate() + 1);
        searchDate = d.toISOString().split("T")[0];
      }

      const logsResult = await pool.query(
        `SELECT msisdn, message_content 
                 FROM sms_logs 
                 WHERE keyword_id = $1 
                 AND received_at >= $2::timestamp + $3::interval
                 AND received_at <= $2::timestamp + $4::interval`,
        [kw.id, searchDate, slot.start, slot.end]
      );

      const filteredNumbers = logsResult.rows
        .filter((row) => {
          const msg = (row.message_content || "")
            .toString()
            .trim()
            .toUpperCase();
          const cleanKw = kw.name.toUpperCase().trim();

          // Check for "AA", "AA SSTV", or just "SSTV" (covers lowercase/uppercase)
          const matchesKeyword = msg === cleanKw;
          const matchesKeywordWithSSTV =
            msg.startsWith(`${cleanKw} SSTV`) ||
            msg.startsWith(`${cleanKw}SSTV`);
          const isSSTV = msg === "SSTV";

          // Return true if any of these match
          return matchesKeyword || matchesKeywordWithSSTV || isSSTV;
        })
        .map((row) => row.msisdn);

      archive.append(filteredNumbers.join("\r\n"), { name: `${kw.name}.txt` });
    }

    archive.finalize();
  } catch (err) {
    console.error("Slot Export Error:", err);
    res.status(500).send("Error generating export");
  }
});

app.get("/api/programs-comparison", async (req, res) => {
  try {
    const result = await pool.query(`
            SELECT p.id, p.name, 
                   SUM(CASE WHEN s.operator = 'Ooredoo' THEN 1 ELSE 0 END) as ooredoo,
                   SUM(CASE WHEN s.operator = 'Dhiraagu' THEN 1 ELSE 0 END) as dhiraagu,
                   COUNT(s.id) as total
            FROM programs p
            LEFT JOIN keywords k ON k.program_id = p.id
            LEFT JOIN sms_logs s ON s.keyword_id = k.id
            GROUP BY p.id, p.name
            ORDER BY total DESC
        `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/heatmap/:programId", async (req, res) => {
  try {
    const { programId } = req.params;
    const query = `
            SELECT 
                TRIM(TO_CHAR(received_at, 'Day')) as day,
                EXTRACT(DOW FROM received_at) as dow,
                EXTRACT(HOUR FROM received_at) as hour,
                COUNT(*) as count
            FROM sms_logs s
            JOIN keywords k ON s.keyword_id = k.id
            WHERE k.program_id = $1
            GROUP BY day, dow, hour
            ORDER BY dow ASC, hour ASC;
        `;
    const result = await pool.query(query, [programId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/programs/:id", async (req, res) => {
  await pool.query("DELETE FROM programs WHERE id = $1", [req.params.id]);
  io.emit("dataUpdated");
  res.json({ success: true });
});

// --- API: KEYWORDS ---
app.get("/api/keywords/:programId", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM keywords WHERE program_id = $1 ORDER BY active_time ASC, name ASC",
      [req.params.programId]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/keywords", async (req, res) => {
  try {
    const { name, program_id, active_time } = req.body;
    const timeValue =
      active_time && active_time.trim() !== "" ? active_time : null;

    await pool.query(
      "INSERT INTO keywords (name, program_id, active_time) VALUES ($1, $2, $3)",
      [name.toUpperCase().trim(), parseInt(program_id), timeValue]
    );
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/keywords/:id", async (req, res) => {
  await pool.query("DELETE FROM keywords WHERE id = $1", [req.params.id]);
  io.emit("dataUpdated");
  res.json({ success: true });
});

// --- API: ANALYTICS ---
app.get("/api/stats/:programId", async (req, res) => {
  const query = `
        SELECT k.name as keyword, k.active_time,
        COUNT(s.id) as count,
        COUNT(s.id) FILTER (WHERE s.operator = 'Ooredoo') as ooredoo,
        COUNT(s.id) FILTER (WHERE s.operator = 'Dhiraagu') as dhiraagu
        FROM keywords k
        LEFT JOIN sms_logs s ON k.id = s.keyword_id
        WHERE k.program_id = $1
        GROUP BY k.name, k.active_time
        ORDER BY k.active_time ASC NULLS LAST;
    `;
  const result = await pool.query(query, [req.params.programId]);
  res.json(result.rows);
});

app.get("/api/daily-stats/:programId", async (req, res) => {
  const query = `
        SELECT TO_CHAR(s.received_at, 'YYYY-MM-DD') as day, k.name as keyword, COUNT(s.id) as count
        FROM keywords k
        JOIN sms_logs s ON k.id = s.keyword_id
        WHERE k.program_id = $1
        GROUP BY day, k.name ORDER BY day ASC;
    `;
  const result = await pool.query(query, [req.params.programId]);
  res.json(result.rows);
});

app.get("/api/global-daily-stats", async (req, res) => {
  const result = await pool.query(
    "SELECT TO_CHAR(received_at, 'YYYY-MM-DD') as day, COUNT(id) as count FROM sms_logs GROUP BY day ORDER BY day ASC"
  );
  res.json(result.rows);
});

// --- CSV UPLOAD ---
app.post("/upload", upload.single("file"), (req, res) => {
  const { operator, keyword_id } = req.body;
  const rows = [];
  fs.createReadStream(req.file.path)
    .pipe(csv({ headers: false }))
    .on("data", (data) => rows.push(Object.values(data)))
    .on("end", async () => {
      let dIdx = -1,
        mIdx = -1,
        msgIdx = -1;
      for (let i = 0; i < rows.length; i++) {
        const line = rows[i].map((c) => String(c).toLowerCase().trim());
        dIdx = line.findIndex((c) => c.includes("date") || c.includes("time"));
        mIdx = line.findIndex(
          (c) =>
            c.includes("mobile") || c.includes("msisdn") || c.includes("number")
        );
        msgIdx = line.findIndex(
          (c) => c.includes("message") || c.includes("response")
        );

        if (dIdx !== -1 && mIdx !== -1) {
          for (let j = i + 1; j < rows.length; j++) {
            const dataRow = rows[j];
            let rDate = dataRow[dIdx],
              rMsisdn = dataRow[mIdx];
            let rMsg = msgIdx !== -1 ? dataRow[msgIdx] : "";

            if (rDate && rMsisdn) {
              let cDate = rDate.trim();
              if (
                cDate.includes("/") ||
                (operator === "Dhiraagu" && cDate.includes("-"))
              ) {
                const sep = cDate.includes("/") ? "/" : "-";
                const pts = cDate.split(" "),
                  dPts = pts[0].split(sep);
                if (dPts[0].length <= 2)
                  cDate = `${dPts[2]}-${dPts[1]}-${dPts[0]} ${
                    pts[1] || "00:00:00"
                  }`;
              }
              try {
                await pool.query(
                  "INSERT INTO sms_logs (received_at, msisdn, keyword_id, operator, message_content) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                  [cDate, rMsisdn.trim(), parseInt(keyword_id), operator, rMsg]
                );
              } catch (e) {}
            }
          }
          break;
        }
      }
      fs.unlinkSync(req.file.path);
      io.emit("dataUpdated");
      res.json({ success: true });
    });
});

io.on("connection", (socket) => {
  socket.on("syncProgress", (data) => {
    io.emit("statusUpdate", data);
  });
});

server.listen(3007, () => console.log(`🚀 Server on http://localhost:3007`));
