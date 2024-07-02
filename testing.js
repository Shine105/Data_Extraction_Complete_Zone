const fs = require('fs');
const path = require('path');
const xlsx = require('node-xlsx');
const csvWriter = require('csv-writer').createObjectCsvWriter;

const inputDirectory = 'HSN_15102023'; // Directory containing input .xls files
const outputDirectory = 'output'; // Directory to store output files
const logFile = 'processing_log.txt'; // Log file for process logs
const errorLogFile = 'error_log.txt'; // Log file for error logs
const batchPrefix = 'Batch_'; // Prefix for batch output files
const batchSize = 10; // Number of files to process per batch

// Enhanced logging function
function log(message) {
    console.log(message);
    fs.appendFileSync(logFile, `${new Date().toISOString()} - ${message}\n`);
}

// Enhanced error logging function
function logError(message) {
    console.error(message);
    fs.appendFileSync(errorLogFile, `${new Date().toISOString()} - ERROR: ${message}\n`);
}

try {
    log('Script execution started.');

    // Ensure output directory exists
    if (!fs.existsSync(outputDirectory)) {
        fs.mkdirSync(outputDirectory);
    }

    // Read all .xls files from the input directory
    const files = fs.readdirSync(inputDirectory).filter(file => path.extname(file) === '.xls');
    log(`${files.length} files found in directory.`);

    let batchCounter = 0;
    let currentBatch = [];
    
    // Organize files into batches
    files.forEach((file, index) => {
        if (index % batchSize === 0 && index > 0) {
            processBatch(currentBatch, batchCounter);
            currentBatch = [];
            batchCounter++;
        }
        currentBatch.push(file);
    });

    if (currentBatch.length > 0) {
        processBatch(currentBatch, batchCounter);
    }

    log('All batches processed. Now combining batch files into final CSV.');

    const combinedCsvPath = path.join(outputDirectory, 'Combined_Extracted_SCADA_Tag_Data.csv');
    combineBatchFiles(combinedCsvPath);

    log('Script execution completed successfully.');

} catch (error) {
    logError(`Script execution failed: ${error.message}`);
}

function processBatch(batchFiles, batchNumber) {
    try {
        log(`Processing batch ${batchNumber + 1} with ${batchFiles.length} files.`);
        
        const batchData = [];
        
        batchFiles.forEach(file => {
            const filePath = path.join(inputDirectory, file);
            log(`Processing file: ${filePath}`);

            const workbook = xlsx.parse(filePath);
            const sheet = workbook[0].data;

            // Define the rows we are interested in
            const rowsOfInterest = [2, 2002, 4002, 6002, 8002];

            const scadaTags = [];
            const uniqueTags = new Set();

            // Iterate over each row of interest separately
            rowsOfInterest.forEach(rowIndex => {
                const row = sheet[rowIndex] || []; // Get the row or an empty array if the row doesn't exist
                row.forEach((cell, cellIndex) => {
                    if (typeof cell === 'string' && !cell.includes('DUMMY')) {
                        if (!uniqueTags.has(cell)) {
                            uniqueTags.add(cell);
                            scadaTags.push({
                                value: cell,
                                rowIndex: rowIndex,
                                columnIndex: cellIndex
                            });
                            log(`Identified SCADA tag: "${cell}" at row ${rowIndex + 1}, column ${cellIndex + 1}`);
                        }
                    }
                });
            });

            if (scadaTags.length === 0) {
                log(`SCADA data column not found in ${file}.`);
                return;
            }

            log(`SCADA data columns found in ${file}.`);

            scadaTags.forEach(tag => {
                const dataValues = [];
                for (let i = 0; i < 1440; i++) {
                    const dataRowIndex = tag.rowIndex + 3 + i; // Adjust as per your specific needs
                    const scadaDataValue = (sheet[dataRowIndex] || [])[tag.columnIndex] || '';
                    dataValues.push(scadaDataValue);
                }
                tag.data = dataValues;
                log(`Extracted SCADA data values from row ${tag.rowIndex + 4} to row ${tag.rowIndex + 3 + 1440}, column ${tag.columnIndex + 1} in ${file}`);
            });

            batchData.push({
                fileName: file,
                tags: scadaTags
            });

            log(`Extracted ${scadaTags.length} SCADA tags and data from ${file}.`);
        });

        const batchCsvPath = path.join(outputDirectory, `${batchPrefix}${batchNumber + 1}_Extracted_SCADA_Tag_Data.csv`);
        writeCsv(batchCsvPath, batchData);
        
        log(`Batch ${batchNumber + 1} SCADA Tag data has been successfully written to ${batchCsvPath}.`);

    } catch (error) {
        logError(`Error processing batch ${batchNumber + 1}: ${error.message}`);
    }
}

function writeCsv(outputPath, data) {
    try {
        if (data.length === 0) {
            log(`No data to write for ${outputPath}.`);
            return;
        }

        const writer = csvWriter({
            path: outputPath,
            header: ['File Name', 'SCADA Tag', 'Row Index', 'Column Index', 'Data']
        });

        const records = data.flatMap(entry => 
            entry.tags.flatMap(tag => 
                tag.data.map((value, index) => ({
                    'File Name': entry.fileName,
                    'SCADA Tag': tag.value,
                    'Row Index': tag.rowIndex + 1,
                    'Column Index': tag.columnIndex + 1,
                    'Data': value
                }))
            )
        );

        writer.writeRecords(records)
            .then(() => log(`Successfully wrote data to ${outputPath}.`))
            .catch(error => logError(`Error writing CSV to ${outputPath}: ${error.message}`));

    } catch (error) {
        logError(`Error in writeCsv function for ${outputPath}: ${error.message}`);
    }
}

function combineBatchFiles(outputPath) {
    try {
        const batchFiles = fs.readdirSync(outputDirectory)
                            .filter(file => file.startsWith(batchPrefix))
                            .sort((a, b) => {
                                const numA = parseInt(a.match(/\d+/)[0], 10);
                                const numB = parseInt(b.match(/\d+/)[0], 10);
                                return numA - numB;
                            });
        
        if (batchFiles.length === 0) {
            logError('No batch files found for combination.');
            return;
        }

        log(`Combining ${batchFiles.length} batch files into ${outputPath}.`);

        // Open the output CSV file for writing
        const outputStream = fs.createWriteStream(outputPath, { flags: 'a' });

        // Write the header to the output file
        outputStream.write('File Name,SCADA Tag,Row Index,Column Index,Data\n');

        let filesProcessed = 0;

        batchFiles.forEach(batchFile => {
            const batchFilePath = path.join(outputDirectory, batchFile);
            const inputStream = fs.createReadStream(batchFilePath, { encoding: 'utf8' });

            inputStream.on('data', (chunk) => {
                outputStream.write(chunk);
            });

            inputStream.on('end', () => {
                log(`${batchFile} has been processed.`);
                filesProcessed++;
                if (filesProcessed === batchFiles.length) {
                    outputStream.end();
                }
            });

            inputStream.on('error', (err) => {
                logError(`Error reading ${batchFile}: ${err.message}`);
            });
        });

        outputStream.on('finish', () => {
            log('All batch files have been combined.');
        });

        outputStream.on('error', (err) => {
            logError(`Error writing to ${outputPath}: ${err.message}`);
        });

    } catch (error) {
        logError(`Error combining batch files: ${error.message}`);
    }
}
