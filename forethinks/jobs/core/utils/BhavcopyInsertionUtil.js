const {spawn} = require("child_process");
const fs = require("fs");
const sql = require("mssql");
const path = require("path");

const logFileDirectory = "./logs/application_log.txt";
const conf = {
	user: "medha",
	password: "medha@feb2023",
	server: "192.168.29.237",
	database: "ForeThinks",
	options: {
		trustServerCertificate: true,
		enableArithAbort: true,
		requestTimeout: 300000, // 5 minutes
	},
};

function addLog(msg) {
	try {
		fs.appendFileSync(logFileDirectory, msg);
	} catch (error) {
		console.log(msg);
	}
}

async function insertSpotData() {
	try {
		await sql.connect(conf);
		const data = await fs.promises.readFile(
			"D:\\ForeThinks\\server\\forethinks\\temp_data\\spot_data\\spot_data.csv",
			"utf8"
		);
		const rows = data.split(/\r?\n/);
		const headers = rows
			.shift()
			.split(",")
			.map((header) => header.trim());
		const records = rows.reduce((records, row) => {
			if (row.trim().length > 0) {
				const columns = row.split(",");
				const record = headers.reduce((record, header, index) => {
					if (columns[index] !== undefined) {
						record[header] = columns[index].replace(/\r/g, "");
					}
					return record;
				}, {});
				records.push(record);
			}
			return records;
		}, []);
		const transaction = new sql.Transaction();
		await transaction.begin();
		try {
			await transaction.request().query`TRUNCATE TABLE BNF_Index`;
			const recordsToInsert = [];
			for (const record of records) {
				if (record.INDEX_NAME) {
					const date = new Date(record.HistoricalDate);
					const year = date.getFullYear();
					const month = (date.getMonth() + 1).toString().padStart(2, "0");
					const day = date.getDate().toString().padStart(2, "0");
					const formattedDate = `${year}-${month}-${day}`;
					recordsToInsert.push({
						INDEX_NAME: record.INDEX_NAME,
						HistoricalDate: formattedDate,
						OPEN: parseFloat(record.OPEN),
						HIGH: parseFloat(record.HIGH),
						LOW: parseFloat(record.LOW),
						CLOSE: parseFloat(record.CLOSE),
					});
				}
			}
			const table = new sql.Table("BNF_Index");
			table.columns.add("INDEX_NAME", sql.VarChar(50), {nullable: true});
			table.columns.add("HistoricalDate", sql.Date, {nullable: true});
			table.columns.add("OPEN", sql.Float, {nullable: true});
			table.columns.add("HIGH", sql.Float, {nullable: true});
			table.columns.add("LOW", sql.Float, {nullable: true});
			table.columns.add("CLOSE", sql.Float, {nullable: true});
			for (const record of recordsToInsert) {
				const indexName = String(record.INDEX_NAME).substring(0, 50);
				table.rows.add(
					indexName,
					record.HistoricalDate,
					record.OPEN,
					record.HIGH,
					record.LOW,
					record.CLOSE
				);
			}
			const request = new sql.Request(transaction);
			await request.bulk(table);
			await transaction.commit();
		} catch (err) {
			console.error(err);
			try {
				await transaction.rollback();
			} catch (err) {
				console.log(
					"error occurred while rolling back spot data insertion transaction"
				);
			}
		}
	} catch (err) {
		console.log("error occured while establishing connection err: " + err);
	} finally {
		sql.close();
	}
}

async function insertFnfSpotData() {
	try {
		await sql.connect(conf);
		const data = await fs.promises.readFile(
			"D:\\ForeThinks\\server\\forethinks\\temp_data\\spot_data\\fnf_data.csv",
			"utf8"
		);
		const rows = data.split(/\r?\n/);
		const headers = rows
			.shift()
			.split(",")
			.map((header) => header.trim());
		const records = rows.reduce((records, row) => {
			if (row.trim().length > 0) {
				const columns = row.split(",");
				const record = headers.reduce((record, header, index) => {
					if (columns[index] !== undefined) {
						record[header] = columns[index].replace(/\r/g, "");
					}
					return record;
				}, {});
				records.push(record);
			}
			return records;
		}, []);
		const transaction = new sql.Transaction();
		await transaction.begin();
		try {
			await transaction.request().query`TRUNCATE TABLE FNF_Index`;
			const recordsToInsert = [];
			for (const record of records) {
				if (record.INDEX_NAME) {
					const date = new Date(record.HistoricalDate);
					const year = date.getFullYear();
					const month = (date.getMonth() + 1).toString().padStart(2, "0");
					const day = date.getDate().toString().padStart(2, "0");
					const formattedDate = `${year}-${month}-${day}`;
					recordsToInsert.push({
						INDEX_NAME: record.INDEX_NAME,
						HistoricalDate: formattedDate,
						OPEN: parseFloat(record.OPEN),
						HIGH: parseFloat(record.HIGH),
						LOW: parseFloat(record.LOW),
						CLOSE: parseFloat(record.CLOSE),
					});
				}
			}
			const table = new sql.Table("FNF_Index");
			table.columns.add("INDEX_NAME", sql.VarChar(50), {nullable: true});
			table.columns.add("HistoricalDate", sql.Date, {nullable: true});
			table.columns.add("OPEN", sql.Float, {nullable: true});
			table.columns.add("HIGH", sql.Float, {nullable: true});
			table.columns.add("LOW", sql.Float, {nullable: true});
			table.columns.add("CLOSE", sql.Float, {nullable: true});
			for (const record of recordsToInsert) {
				const indexName = String(record.INDEX_NAME).substring(0, 50);
				table.rows.add(
					indexName,
					record.HistoricalDate,
					record.OPEN,
					record.HIGH,
					record.LOW,
					record.CLOSE
				);
			}
			const request = new sql.Request(transaction);
			await request.bulk(table);
			await transaction.commit();
		} catch (err) {
			console.error(err);
			try {
				await transaction.rollback();
			} catch (err) {
				console.log(
					"error occurred while rolling back fnf spot data insertion transaction"
				);
			}
		}
	} catch (err) {
		console.log("error occured while establishing connection err: " + err);
	} finally {
		sql.close();
	}
}

async function insertBhavCopyIntoDB(startDate, retries = 3) {
	new Promise((resolve, reject) => {
		const insertionScriptPath = "./scripts/script2.py";
		const process = spawn("python", [insertionScriptPath]);
		let errorMessage = "";
		process.stdout.on("data", (data) => {
			console.log(`stdout: ${data}`);
		});
		process.stderr.on("data", (data) => {
			errorMessage += data;
		});
		process.on("close", async (code) => {
			if (code !== 0 || errorMessage !== "") {
				console.log(
					"last transaction while inserting data could not be rolled back, going to validate inserted data in 10 minutes, check and establish connection between db"
				);
				let validateRetries = 3;
				while (validateRetries > 0) {
					// wait for 10 minutes before validating
					await new Promise((resolve) => setTimeout(resolve, 10 * 60 * 1000));
					try {
						await validate();
						console.log(
							"successfully validated, going to retry insertion for uninserted data"
						);
						insertBhavCopyIntoDB(retries - 1);
						resolve();
						return;
					} catch (error) {
						if (validateRetries === 1) {
							console.log(
								"could not validate, validation is scheduled to happen in 1 hour from now"
							);
						}
						validateRetries--;
					}
				}
			} else {
				let validateRetries = 3;
				while (validateRetries > 0) {
					try {
						await validate();
						if (await checkForCSVFiles()) {
							insertBhavCopyIntoDB(retries - 1);
						}
						resolve();
						return;
					} catch (error) {
						if (retries === 1) {
							console.log(
								"could not validate, run validation after establishing connection : " +
									error
							);
						}
						validateRetries--;
					}
				}
			}
		});
	});
}

async function checkForCSVFiles() {
	const directory = "D:\\ForeThinks\\server\\forethinks\\temp_data";
	// Read contents of directory
	const files = await fs.promises.readdir(directory);
	// Check if any file is a CSV file
	return files.some((file) => path.extname(file) === ".csv");
}

async function validate() {
	try {
		await sql.connect(conf);
		const csvDirectoryPath = "D:\\ForeThinks\\server\\forethinks\\temp_data";
		const dates = [];
		try {
			const files = fs.readdirSync(csvDirectoryPath);
			for (const file of files) {
				if (file.endsWith(".csv")) {
					console.log(getDateFromFileName);
					const date = getDateFromFileName(file);
					dates.push(date);
				}
			}
		} catch (error) {
			console.log("error occurred while reading csv file names from tempDir");
		}
		const result =
			await sql.query`SELECT DATE FROM BhavCopyDownloadAudit WHERE DATE IN (${dates}) AND INSERT_INTO_RAWDATA_STATUS = 1 AND VERIFIED_STATUS = 0`;

		const dbDates = result.recordset.map((record) => record.DATE);
		const monthNames = [
			"Jan",
			"Feb",
			"Mar",
			"Apr",
			"May",
			"Jun",
			"Jul",
			"Aug",
			"Sep",
			"Oct",
			"Nov",
			"Dec",
		];
		// Loop through each date
		for (const date of dbDates) {
			console.log(date);
			// Generate file name
			const day = date.getDate().toString().padStart(2, "0");
			const month = monthNames[date.getMonth()];
			const year = date.getFullYear();
			const fileName = `fo${day}${month}${year}bhav.csv`;

			// Read CSV file
			const filePath = `D:\\ForeThinks\\server\\forethinks\\temp_data\\${fileName}`;
			const data = fs.readFileSync(filePath, "utf8");
			const rows = data.split("\n");

			// Get number of rows where SYMBOL column has non-empty value
			let rowCount = 0;
			for (const row of rows) {
				if (row.split(",")[1]) rowCount++;
			}
			rowCount--;

			// Fetch record count from BhavCopyDownloadAudit table
			const dbResult =
				await sql.query`SELECT TotalRecords FROM BhavCopyDownloadAudit WHERE DATE = ${date}`;
			const recordCount = parseInt(dbResult.recordset[0].TotalRecords);
			console.log(
				"totalRecords from DB : " +
					recordCount +
					"row count from csv : " +
					rowCount
			);
			// Check if row count matches record count in the db for the date
			if (rowCount === recordCount) {
				// Update verified_status to 1 and delete file from directory
				await sql.query`UPDATE BhavCopyDownloadAudit SET VERIFIED_STATUS = 1 WHERE DATE  = ${date}`;
				fs.unlinkSync(filePath);
				console.log("file deleted successfully");
			} else {
				// Update insertion_status to 0
				await sql.query`UPDATE BhavCopyDownloadAudit SET INSERT_INTO_RAWDATA_STATUS = 0 WHERE DATE = ${date}`;
				// Delete all data from RawData table where TIMESTAMP = date
				await sql.query`DELETE FROM RawData WHERE TIMESTAMP = ${date}`;
			}
		}
	} catch (err) {
		throw err;
	} finally {
		sql.close();
	}
}

function getDateFromFileName(filename) {
	const cleanedFileName = filename.replace(/fo|bhav|.csv/g, "");
	const dateObj = new Date(Date.parse(cleanedFileName));
	const year = dateObj.getFullYear();
	const month = (dateObj.getMonth() + 1).toString().padStart(2, "0");
	const day = dateObj.getDate().toString().padStart(2, "0");
	return `${year}-${month}-${day}`;
}

module.exports = {
	insertBhavCopyIntoDB,
	insertSpotData,
	insertFnfSpotData,
};
