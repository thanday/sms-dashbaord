const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const AdmZip = require('adm-zip'); // New dependency for extraction
const { Pool } = require('pg');
const { io: clientIo } = require("socket.io-client");

const socket = clientIo("http://localhost:3007");
const targetOperator = process.argv[2]; 

const pool = new Pool({
    user: 'engineering',
    host: 'localhost',
    database: 'sms_stats',
    password: 'Sun.Media@94.6', // Ensure this is exactly like your server.js
    port: 5432,
});

const downloadPath = path.resolve(__dirname, 'temp_downloads');
if (!fs.existsSync(downloadPath)) fs.mkdirSync(downloadPath);

/**
 * OOREDOO SCRAPER 
 * Restored working version (Standard CSV)
 */
async function scrapeOoredoo() {
    const oldFiles = fs.readdirSync(downloadPath);
    for (const file of oldFiles) {
        try { fs.unlinkSync(path.join(downloadPath, file)); } catch (e) {}
    }

    const browser = await puppeteer.launch({ 
        headless: true, 
        defaultViewport: null,
        args: ['--start-maximized', '--no-sandbox']
    });
    
    const page = await browser.newPage();
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath });

    try {
        console.log("--- Starting Ooredoo Sync ---");
        socket.emit('syncProgress', { percent: 5, status: 'Ooredoo: Logging in...' });

        await page.goto('https://www.ooredoo.mv/webapps/tv-radio/public/login', { waitUntil: 'networkidle0' });
        await page.type('input[name="username"]', 'sunfm');
        await page.type('input[name="password"]', 'j~6GQ?-56');

        await page.evaluate(() => {
            const btn = document.querySelector('button[type="submit"]') || document.querySelector('.btn-block');
            if (btn) btn.click();
        });

        await page.waitForNavigation({ waitUntil: 'networkidle0' });
        socket.emit('syncProgress', { percent: 15, status: 'Ooredoo: Logged in' });

        const localKeywords = await pool.query('SELECT id, name FROM keywords');
        const today = new Date().toISOString().split('T')[0];

        for (let i = 0; i < localKeywords.rows.length; i++) {
            const kw = localKeywords.rows[i];
            const progress = Math.round(15 + ((i / localKeywords.rows.length) * 80));
            socket.emit('syncProgress', { percent: progress, status: `Ooredoo: Downloading ${kw.name}...` });

            await page.goto('https://www.ooredoo.mv/webapps/tv-radio/public/view_statistics', { waitUntil: 'networkidle0' });
            await page.waitForSelector('#keyword');
            await page.select('#keyword', kw.name);

            await page.evaluate((start, end) => {
                document.getElementById('start_time').value = start;
                document.getElementById('end_time').value = end;
            }, '2026-02-18', today);

            await page.click('#download');

            try {
                const fileName = await waitForFile(downloadPath, '.csv');
                const filePath = path.join(downloadPath, fileName);
                await processOoredooCSV(filePath, kw.id);
                fs.unlinkSync(filePath); 
            } catch (fileErr) {
                console.error(`Skip ${kw.name}: ${fileErr.message}`);
            }
        }
        socket.emit('syncProgress', { percent: 100, status: 'Ooredoo Sync Complete!', done: true });
    } catch (err) {
        socket.emit('syncProgress', { percent: 0, status: 'Ooredoo Error: ' + err.message, done: true });
    } finally {
        await browser.close();
    }
}

/**
 * DHIRAAGU SCRAPER: Final Resilient Frame Version
 */
async function scrapeDhiraagu() {
    const browser = await puppeteer.launch({ 
        headless: true, 
        args: ['--start-maximized', '--no-sandbox'] 
    });
    
    const targetDateInt = 20260218;

    try {
        console.log("--- Starting Dhiraagu Sync (Stable Isolation) ---");
        const localKeywords = await pool.query('SELECT id, name FROM keywords');

        for (let i = 0; i < localKeywords.rows.length; i++) {
            const kw = localKeywords.rows[i];
            socket.emit('syncProgress', { 
                percent: Math.round(10 + (i / localKeywords.rows.length) * 85), 
                status: `Dhiraagu: ${kw.name} (Isolated Session)` 
            });

            // 1. CLEAR old files
            fs.readdirSync(downloadPath).forEach(f => {
                if (f.endsWith('.zip')) try { fs.unlinkSync(path.join(downloadPath, f)); } catch(e) {}
            });

            // 2. FRESH CONTEXT
            const context = await browser.createBrowserContext();
            const loginPage = await context.newPage();
            const client = await loginPage.target().createCDPSession();
            await client.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath });

            try {
                // 3. LOGIN - Use 'networkidle0' to ensure page is fully loaded
                await loginPage.goto('https://corporatecms.dhiraagu.com.mv/', { waitUntil: 'networkidle0' });
                
                await loginPage.mouse.click(400, 300); 
                await loginPage.keyboard.press('Tab');
                await loginPage.keyboard.type('SUNCHANNEL');
                await loginPage.keyboard.press('Tab');
                await loginPage.keyboard.type('Sunmedia123');

                // 4. CAPTURE POPUP - Wait for the target specifically
                const popupPromise = new Promise(resolve => context.once('targetcreated', target => resolve(target.page())));
                await loginPage.keyboard.press('Enter');
                const popup = await popupPromise;

                // Crucial: Wait for the popup to finish its internal redirects
                await popup.waitForNavigation({ waitUntil: 'networkidle2' }).catch(() => {});
                
                const popupClient = await popup.target().createCDPSession();
                await popupClient.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadPath });
                await popup.setViewport({ width: 1440, height: 900 });

                // 5. MIS -> REPORTS (Recursive Frame Search)
                // We use a small delay to prevent "Context Destroyed"
                await new Promise(r => setTimeout(r, 2000));
                await popup.evaluate(async () => {
                    const findAndClick = (doc, text) => {
                        const links = Array.from(doc.querySelectorAll('a, b, span'));
                        const target = links.find(el => el.textContent.trim() === text);
                        if (target) { target.click(); return true; }
                        for (let f of doc.querySelectorAll('frame, iframe')) {
                            try { if (findAndClick(f.contentDocument, text)) return true; } catch(e){}
                        }
                    };
                    findAndClick(document, 'MIS');
                    // Short internal delay for MIS menu expansion
                    await new Promise(r => setTimeout(r, 1000));
                    findAndClick(document, 'Reports');
                });

                // Wait for the Reports form frame to load
                await new Promise(r => setTimeout(r, 6000));

                // 6. SELECT KEYWORD
                await popup.evaluate((keyword) => {
                    const getUI = (doc) => {
                        const selects = doc.querySelectorAll('select');
                        if (selects.length >= 2) return { selects, submit: doc.querySelector('input[type="submit"]') };
                        for (let f of doc.querySelectorAll('frame, iframe')) {
                            try { const res = getUI(f.contentDocument); if (res) return res; } catch(e){}
                        }
                    };
                    const ui = getUI(document);
                    if (ui) {
                        ui.selects[0].value = Array.from(ui.selects[0].options).find(o => o.text.includes('SUNCHANNEL'))?.value;
                        ui.selects[0].dispatchEvent(new Event('change', { bubbles: true }));

                        setTimeout(() => {
                            const kwOpt = Array.from(ui.selects[1].options).find(o => o.text.endsWith(' - ' + keyword));
                            if (kwOpt) {
                                ui.selects[1].value = kwOpt.value;
                                ui.selects[1].dispatchEvent(new Event('change', { bubbles: true }));
                                setTimeout(() => ui.submit?.click(), 1000);
                            }
                        }, 2000);
                    }
                }, kw.name);

                // 7. DOWNLOAD SEQUENCE
                await new Promise(r => setTimeout(r, 12000));
                await popup.evaluate(() => {
                    const clickBtn = (doc, text) => {
                        const btns = Array.from(doc.querySelectorAll('input, button, a'));
                        const target = btns.find(b => (b.value || b.textContent || "").toLowerCase().includes(text));
                        if (target) { target.click(); return true; }
                        for (let f of doc.querySelectorAll('frame, iframe')) {
                            try { if (clickBtn(f.contentDocument, text)) return true; } catch(e){}
                        }
                    };
                    clickBtn(document, 'download csv');
                });

                await new Promise(r => setTimeout(r, 6000));
                await popup.evaluate(() => {
                    const clickFinal = (doc) => {
                        const links = Array.from(doc.querySelectorAll('a'));
                        const target = links.find(l => l.textContent.toLowerCase().includes('click here'));
                        if (target) { target.click(); return true; }
                        for (let f of doc.querySelectorAll('frame, iframe')) {
                            try { if (clickFinal(f.contentDocument)) return true; } catch(e){}
                        }
                    };
                    clickFinal(document);
                });

                // 8. FILE PROCESSING
                const zipName = await waitForFile(downloadPath, '.zip');
                const zipPath = path.join(downloadPath, zipName);
                const zip = new AdmZip(zipPath);
                const csvEntry = zip.getEntries().find(e => e.entryName.endsWith('.csv'));

                if (csvEntry) {
                    const tempPath = path.join(downloadPath, `temp_${kw.id}.csv`);
                    fs.writeFileSync(tempPath, zip.readAsText(csvEntry));
                    await processDhiraaguCSV(tempPath, kw.id, targetDateInt);
                    fs.unlinkSync(tempPath);
                }
                fs.unlinkSync(zipPath);
                console.log(`Dhiraagu: Successfully finished ${kw.name}`);

            } catch (err) {
                console.log(`Dhiraagu: Error on ${kw.name}: ${err.message}`);
            } finally {
                // Close context to free memory and reset session completely
                await context.close();
            }
        }
        socket.emit('syncProgress', { percent: 100, status: 'Dhiraagu Sync Complete!', done: true });
    } finally {
        await browser.close();
    }
}
/**
 * UTILITIES
 */
async function waitForFile(dir, ext) {
    return new Promise((resolve, reject) => {
        let sec = 0;
        const interval = setInterval(() => {
            sec++;
            const file = fs.readdirSync(dir).find(f => f.endsWith(ext) && !f.includes('.crdownload'));
            if (file) { clearInterval(interval); setTimeout(() => resolve(file), 2000); }
            if (sec > 30) { clearInterval(interval); reject(new Error("Timeout waiting for " + ext)); }
        }, 1000);
    });
}

async function processOoredooCSV(filePath, keywordId) {
    const results = [];
    return new Promise((resolve) => {
        fs.createReadStream(filePath)
            .pipe(csv({ mapHeaders: ({ header }) => header.trim() }))
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                for (const row of results) {
                    const time = row['Date Time'];
                    const phone = row['Msisdn'];
                    const msg = row['Response'] || ""; // Capture message content

                    if (time && phone) {
                        try {
                            await pool.query(
                                `INSERT INTO sms_logs (received_at, msisdn, keyword_id, operator, message_content) 
                                 VALUES ($1, $2, $3, $4, $5) 
                                 ON CONFLICT (received_at, msisdn) 
                                 DO UPDATE SET message_content = EXCLUDED.message_content`, 
                                [time, phone, keywordId, 'Ooredoo', msg.trim()]
                            );
                        } catch (e) {
                            console.error("Ooredoo Error:", e.message);
                        }
                    }
                }
                resolve();
            });
    });
}

async function processDhiraaguCSV(filePath, keywordId, targetDateInt) {
    const results = [];
    return new Promise((resolve) => {
        fs.createReadStream(filePath)
            .pipe(csv({ headers: false }))
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                for (let i = 2; i < results.length; i++) {
                    const row = results[i];
                    const rawDate = row[1]?.trim();
                    const rawPhone = row[2]?.trim();
                    const rawMsg = row[3]?.trim() || "";

                    if (rawDate && rawPhone) {
                        const match = rawDate.match(/^(\d{2})-(\d{2})-(\d{4})\s(\d{2}:\d{2}:\d{2})/);
                        if (match) {
                            const d = match[1], m = match[2], y = match[3], time = match[4];
                            const rowDateInt = parseInt(y + m + d);

                            if (rowDateInt >= targetDateInt) {
                                const finalDate = `${y}-${m}-${d} ${time}`;
                                try {
                                    await pool.query(
                                        `INSERT INTO sms_logs (received_at, msisdn, keyword_id, operator, message_content) 
                                         VALUES ($1, $2, $3, $4, $5) 
                                         ON CONFLICT (received_at, msisdn) 
                                         DO UPDATE SET message_content = EXCLUDED.message_content`,
                                        [finalDate, rawPhone, keywordId, 'Dhiraagu', rawMsg]
                                    );
                                } catch (e) { console.error("Dhiraagu Error:", e.message); }
                            }
                        }
                    }
                }
                resolve();
            });
    });
}

(async () => {
    if (targetOperator === 'ooredoo') await scrapeOoredoo();
    else if (targetOperator === 'dhiraagu') await scrapeDhiraagu();
    await pool.end();
    setTimeout(() => process.exit(0), 1000);
})();