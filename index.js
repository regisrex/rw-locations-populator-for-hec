/**
 * @file This script reads Rwandan geolocation data from a CSV file
 * and populates a database table by making POST requests to a
 * Spring Boot API endpoint.
 *
 * @requires csv-parser
 * @requires axios
 * @requires fs
 * @requires path
 */

const fs = require('fs');
const path = require('path');
const parser = require('csv-parser'); // Destructure parse for direct use
const axios = require('axios');

// --- Configuration ---
const CSV_FILE_PATH = path.join(__dirname, 'rwanda_locations.csv');
const API_BASE_URL = 'http://localhost:8081/api/hec/location'; // Adjust if your base URL is different

/**
 * Parses the CSV file and returns a Promise that resolves with an array of records.
 * @returns {Promise<Array<Object>>} A promise that resolves with the parsed CSV data.
 */
async function readCsvFile() {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(CSV_FILE_PATH)
            .pipe(parser({
                columns: true, // Auto-detect column names from the first row
                skipEmptyLines: true, // Skip any empty lines in the CSV
                trim: true // Trim whitespace from values
            }))
            .on('data', (data) => {
                // Clean up data: remove leading/trailing whitespace from all string values
                for (const key in data) {
                    if (typeof data[key] === 'string') {
                        data[key] = data[key].trim();
                    }
                }
                console.log(data)
                results.push(data);
            })
            .on('end', () => {
                console.log(`Successfully read ${results.length} records from ${CSV_FILE_PATH}`);
                resolve(results);
            })
            .on('error', (error) => {
                console.error(`Error reading CSV file: ${error.message}`);
                reject(error);
            });
    });
}

/**
 * Populates the locations in the database by calling the API.
 * Processes locations hierarchically (Provinces, then Districts, etc.)
 * to ensure parent locations are saved before their children.
 *
 * @param {Array<Object>} locationsData - An array of location objects parsed from the CSV.
 */
async function populateLocations(locationsData) {
    const locationTypesOrder = ['PROVINCE', 'DISTRICT', 'SECTOR', 'CELL', 'VILLAGE'];

    // Sort locations to ensure parents are processed before children
    // This assumes a well-formed CSV where parent codes refer to already defined locations.
    locationsData.sort((a, b) => {
        try {


            const levelA = locationTypesOrder.indexOf(a.Level.toUpperCase());
            const levelB = locationTypesOrder.indexOf(b.Level.toUpperCase());
            return levelA - levelB;
        } catch (error) {
            console.log(a, b)
            console.log(error)
        }
    });

    console.log('Starting population process...');

    for (const location of locationsData) {
        const { Level, LocationCode, LocationName, ParentCode } = location;

        const payload = {
            locationCode: LocationCode,
            locationType: Level.toUpperCase(), // Ensure uppercase as per ELocationType enum
            locationName: LocationName
        };

        // Construct the API URL. Add parentCode as a query parameter if available.
        let apiUrl = `${API_BASE_URL}/saveLocation`;
        if (ParentCode && ParentCode.length > 0) {
            apiUrl += `?parentCode=${encodeURIComponent(ParentCode)}`;
        }

        try {
            console.log(`Attempting to save: ${LocationName} (${LocationCode}), Type: ${Level}, Parent: ${ParentCode || 'None'}`);
            const response = await axios.post(apiUrl, payload, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            console.log(`SUCCESS: ${LocationName} (${LocationCode}) - ${response.data}`);
        } catch (error) {
            if (error.response) {
                // The request was made and the server responded with a status code
                // that falls out of the range of 2xx
                console.error(`ERROR saving ${LocationName} (${LocationCode}): Status ${error.response.status} - ${error.response.data}`);
            } else if (error.request) {
                // The request was made but no response was received
                console.error(`ERROR: No response received for ${LocationName} (${LocationCode}). Is the server running? ${error.message}`);
            } else {
                // Something happened in setting up the request that triggered an Error
                console.error(`ERROR: Request setup failed for ${LocationName} (${LocationCode}): ${error.message}`);
            }
        }
        // Add a small delay to prevent overwhelming the server with requests
        await new Promise(resolve => setTimeout(resolve, 100)); // 100ms delay
    }

    console.log('Population process finished.');
}

/**
 * Main execution function.
 */
async function main() {
    try {
        const locations = await readCsvFile();

        console.log(locations)
        if (locations.length > 0) {
            await populateLocations(locations);
        } else {
            console.warn('No location data found in CSV to populate.');
        }
    } catch (error) {
        console.error('Failed to run population script:', error);
    }
}

// Execute the main function
main();
