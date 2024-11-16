require('dotenv').config();
const dataForge = require('data-forge');
require('data-forge-fs');
const { API_KEY, API_URL, AID } = process.env;
console.log(API_URL)

// Read of initial files. Then inner joins them.
async function readFiles(file_A, file_B) {
    try {
        const series_A = await dataForge.readFile(file_A)
            .parseCSV();
        console.log(`${file_A} loaded`);

        const series_B = await dataForge.readFile(file_B)
            .parseCSV();
        console.log(`${file_B} loaded`);

        var rows_A = series_A.content.values.rows
        var rows_B = series_B.content.values.rows

        const merged_rows = [];
        rows_A.map( (row, index) => {
            rows_B.map( row_B => {
                if( row_B[0] === row[0]) {
                    merged_rows.push( [row[0], row[1], row_B[1], row_B[2]] );
                }
            })
        })
        return merged_rows
        
    } catch (error) {
        console.error('Error during file operations:', error);
        throw error;
    }
}

// Converts given array of data into CSV file
async function dataToCSV(data, file_path) {
    try {
        const csv_data = new dataForge.DataFrame({
            columns: {
                'user_id': data.map(row => row[0]),
                'email': data.map(row => row[1]),
                'first_name': data.map(row => row[2]),
                'last_name': data.map(row => row[3])
            }
        });

        await csv_data.asCSV().writeFile(file_path);
        
    } catch (error) {
        console.error('Error during file operations:', error);
        throw error;
    }
}

// Fetches the URL for the Piano API and tries to find the user through its email, then returns it. Return may be empty.
async function get_user(email) {
    const params = new URLSearchParams({
        "email": email,
        "aid": AID,
        "api_token": API_KEY
    });

    try {
        const response = await fetch(`${API_URL}/publisher/user/search?${params.toString()}`, { method: "GET" });

        if (!response.ok) {
            throw new Error(`Status: ${response.status}`);
        }

        const data = await response.json();
        return data.users[0];
    } catch (error) {
        console.error("Error: ", error);
        throw error;
    }
}

// main
async function main() {
    const file_A = './FILE A.csv';
    const file_B = './FILE B.csv';
    const file_path = './Merged Users.csv';

    try {
        const clean_data = await readFiles(file_A, file_B);
        for (let i = 0; i < clean_data.length; i++ ) {
            let user = await get_user(clean_data[i][1]);
            if (user && user.uid) {
                clean_data[i][0] = user.uid;
            }
        }
        dataToCSV(clean_data, file_path)
        console.log(clean_data)
    } catch (error) {
        console.error('Failed to read files:', error);
    }    
}

main();