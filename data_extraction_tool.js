// Main backend server - Express.js
const express = require('express');
const multer = require('multer');
const JSZip = require('jszip');
const fs = require('fs');
const path = require('path');
const { Parser } = require('json2csv');
const XLSX = require('xlsx');
const pdfParse = require('pdf-parse');
const Fuse = require('fuse.js');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// --- CONFIGURATION ---
const TARGET_COLUMNS = ["DATE", "FULL NAME", "CONTACT", "ADDRESS"];

const COLUMN_MAPPINGS = {
    "DATE": ["date", "dob", "time", "created_at", "joined", "day"],
    "FULL NAME": ["name", "fullname", "client", "customer", "first name", "last name", "user"],
    "CONTACT": ["phone", "mobile", "cell", "tel", "contact number", "number"],
    "ADDRESS": ["address", "location", "residence", "city", "street"]
};

// --- AI/PROCESSING FUNCTIONS ---

function aiMapColumn(header) {
    /**
     * Uses Fuzzy Logic to simulate AI semantic understanding of column headers.
     * Returns the target column name if a match is found with >80% confidence.
     */
    header = String(header).toLowerCase().trim();
    
    // 1. Direct contains check
    for (const [target, synonyms] of Object.entries(COLUMN_MAPPINGS)) {
        if (synonyms.includes(header)) {
            return target;
        }
    }
    
    // 2. Fuzzy Matching (The "AI" intuition)
    let bestMatch = null;
    let highestScore = 0;
    
    for (const [target, synonyms] of Object.entries(COLUMN_MAPPINGS)) {
        const fuse = new Fuse(synonyms, { threshold: 0.2 });
        const results = fuse.search(header);
        
        if (results.length > 0) {
            const score = (1 - results[0].score) * 100;
            if (score > 80 && score > highestScore) {
                highestScore = score;
                bestMatch = target;
            }
        }
    }
    
    return bestMatch;
}

function cleanDate(val) {
    /**Normalize dates to YYYY-MM-DD using flexible parsing.*/
    try {
        const date = new Date(val);
        if (isNaN(date)) return null;
        return date.toISOString().split('T')[0];
    } catch {
        return null;
    }
}

function cleanName(val) {
    /**Remove symbols/numbers, keep only letters and spaces.*/
    if (typeof val !== 'string') return null;
    const clean = val.replace(/[^a-zA-Z\s]/g, '').trim();
    return clean ? clean.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ') : null;
}

function cleanPhone(val) {
    /**Standardize phone numbers to digits only.*/
    if (val === null || val === undefined) return null;
    const digits = String(val).replace(/\D/g, '');
    return digits.length > 5 ? digits : null;
}

function extractTextFromPdf(buffer) {
    /**Extract and parse text from PDF.*/
    return new Promise((resolve, reject) => {
        pdfParse(buffer).then(data => {
            const text = data.text;
            const lines = text.split('\n');
            const records = [];
            let currentRecord = {};
            
            for (const line of lines) {
                if (line.includes("Date:")) currentRecord["DATE"] = line.split("Date:")[1].split("|")[0].trim();
                if (line.includes("Name:")) currentRecord["FULL NAME"] = line.split("Name:")[1].split("|")[0].trim();
                if (line.includes("Phone:")) currentRecord["CONTACT"] = line.split("Phone:")[1].split("|")[0].trim();
                if (line.includes("Address:")) currentRecord["ADDRESS"] = line.split("Address:")[1].trim();
                
                if (Object.keys(currentRecord).length >= 3) {
                    records.push(currentRecord);
                    currentRecord = {};
                }
            }
            resolve(records);
        }).catch(reject);
    });
}

// --- ROUTES ---

app.use(express.static('public'));

app.post('/upload', upload.single('file'), async (req, res) => {
    try {
        let allData = [];
        let filesProcessed = 0;
        
        if (req.file.originalname.endsWith('.zip')) {
            const zip = await JSZip.loadAsync(req.file.buffer);
            
            for (const [filename, file] of Object.entries(zip.files)) {
                if (filename.startsWith("__MACOSX") || filename.endsWith("/")) continue;
                
                const fileExt = filename.split('.').pop().toLowerCase();
                const fileBuffer = await file.async('arraybuffer');
                
                try {
                    let df = null;
                    
                    if (fileExt === 'csv') {
                        df = parseCSV(Buffer.from(fileBuffer).toString('utf-8'));
                    } else if (fileExt === 'xlsx') {
                        df = parseExcel(fileBuffer);
                    } else if (fileExt === 'pdf') {
                        df = await extractTextFromPdf(Buffer.from(fileBuffer));
                    } else {
                        continue;
                    }
                    
                    // Apply AI Column Mapping
                    let renamedCols = {};
                    for (const col of Object.keys(df[0] || {})) {
                        const mapped = aiMapColumn(col);
                        if (mapped) renamedCols[col] = mapped;
                    }
                    
                    df = df.map(row => {
                        let newRow = {};
                        for (const [oldCol, newCol] of Object.entries(renamedCols)) {
                            newRow[newCol] = row[oldCol];
                        }
                        TARGET_COLUMNS.forEach(col => {
                            if (!(col in newRow)) newRow[col] = null;
                        });
                        return newRow;
                    });
                    
                    allData = allData.concat(df);
                    filesProcessed++;
                } catch (e) {
                    console.error(`Error processing ${filename}:`, e);
                }
            }
        } else if (req.file.originalname.endsWith('.csv')) {
            allData = parseCSV(req.file.buffer.toString('utf-8'));
            filesProcessed = 1;
        } else if (req.file.originalname.endsWith('.xlsx')) {
            allData = parseExcel(req.file.buffer);
            filesProcessed = 1;
        }
        
        // --- CLEANING ---
        let cleanedData = allData.map(row => ({
            DATE: cleanDate(row.DATE),
            "FULL NAME": cleanName(row["FULL NAME"]),
            CONTACT: cleanPhone(row.CONTACT),
            ADDRESS: row.ADDRESS || null,
            DATA_STATUS: Object.values(row).some(v => v === null) ? "Missing Fields" : "Valid"
        }));
        
        // Remove duplicates
        const before = cleanedData.length;
        cleanedData = Array.from(new Map(
            cleanedData.map(row => [
                `${row["FULL NAME"]}-${row.CONTACT}-${row.DATE}`,
                row
            ])
        ).values());
        const dedupCount = before - cleanedData.length;
        
        res.json({
            success: true,
            filesProcessed,
            recordCount: cleanedData.length,
            dedupCount,
            data: cleanedData
        });
        
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/download/:format', express.json(), (req, res) => {
    const { format } = req.params;
    const { data } = req.body;
    
    if (format === 'csv') {
        const csv = data.map(row => 
            `${row.DATE},"${row["FULL NAME"]}","${row.CONTACT}","${row.ADDRESS}"`
        ).join('\n');
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="cleaned_data.csv"');
        res.send(`DATE,FULL NAME,CONTACT,ADDRESS\n${csv}`);
    } else if (format === 'xlsx') {
        const ws = XLSX.utils.json_to_sheet(data);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Cleaned Data");
        const buffer = XLSX.write(wb, { bookType: 'xlsx', type: 'buffer' });
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename="cleaned_data.xlsx"');
        res.send(buffer);
    }
});

// Helper functions
function parseCSV(csvString) {
    const lines = csvString.trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim());
    return lines.slice(1).map(line => {
        const values = line.split(',').map(v => v.trim());
        let obj = {};
        headers.forEach((h, i) => obj[h] = values[i] || null);
        return obj;
    });
}

function parseExcel(buffer) {
    const workbook = XLSX.read(buffer, { type: 'array' });
    const wsname = workbook.SheetNames[0];
    return XLSX.utils.sheet_to_json(workbook.Sheets[wsname]);
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));