const {ConnectionPool} = require("mssql");
const fs = require("fs");
const {insertSpotData, insertFnfSpotData} = require("./BhavcopyInsertionUtil");
const {spawn} = require("child_process");

const downloadDir = "./temp_data";
const spotDataDownloaderScriptPath = "./scripts/SpotDataDownloader.py";
const fnfDataDownloaderScriptPath = "./scripts/FNFDownloader.py";
const conf = {
	user: "medha",
	password: "medha@feb2023",
	server: "192.168.29.237",
	database: "ForeThinks",
	options: {
		trustServerCertificate: true,
		enableArithAbort: true,
		requestTimeout: 900000, // 15 minutes
	},
};

const formatDate = (date) => {
	const year = date.getFullYear();
	const month = (date.getMonth() + 1).toString().padStart(2, "0");
	const day = date.getDate().toString().padStart(2, "0");
	return `${year}-${month}-${day}`;
};

async function downloadSpotData() {
	const filePath =
		"D:\\ForeThinks\\server\\forethinks\\temp_data\\spot_data\\spot_data.csv";
	try {
		await fs.promises.unlink(filePath);
	} catch (err) {
		console.log(`Failed to delete file at ${filePath}: ${err}`);
	}
	let startDate = new Date("2019-01-01");
	let currentDate = new Date();
	startDate = formatDate(startDate);
	currentDate = formatDate(currentDate);
	try {
		await new Promise((resolve, reject) => {
			const process = spawn("python", [
				spotDataDownloaderScriptPath,
				startDate,
				currentDate,
			]);
			let errorMessage = "";
			process.stdout.on("data", (data) => {
				console.log(`stdout: ${data}`);
			});
			process.stderr.on("data", (data) => {
				errorMessage += data;
			});
			process.on("close", async (code) => {
				if (code !== 0 || errorMessage !== "") {
					reject(new Error(errorMessage));
				} else {
					await insertSpotData();
					resolve();
				}
			});
		});
	} catch (err) {
		console.log(err);
	}
}

async function downloadSpotDataFNF() {
	const filePath =
		"D:\\ForeThinks\\server\\forethinks\\temp_data\\spot_data\\fnf_data.csv";
	try {
		await fs.promises.unlink(filePath);
	} catch (err) {
		console.log(`Failed to delete file at ${filePath}: ${err}`);
	}
	let startDate = new Date("2019-01-01");
	let currentDate = new Date();
	startDate = formatDate(startDate);
	currentDate = formatDate(currentDate);
	try {
		await new Promise((resolve, reject) => {
			const process = spawn("python", [
				fnfDataDownloaderScriptPath,
				startDate,
				currentDate,
			]);
			let errorMessage = "";
			process.stdout.on("data", (data) => {
				console.log(`stdout: ${data}`);
			});
			process.stderr.on("data", (data) => {
				errorMessage += data;
			});
			process.on("close", async (code) => {
				if (code !== 0 || errorMessage !== "") {
					reject(new Error(errorMessage));
				} else {
					await insertFnfSpotData();
					resolve();
				}
			});
		});
	} catch (err) {
		console.log(err);
	}
}

async function findMaxDate(retryCount = 1, delay = 1000) {
	let pool;
	try {
		pool = await new ConnectionPool(conf).connect();
		const result = await pool.request()
			.query`SELECT MAX(TIMESTAMP) AS maxDate FROM RawData`;
		const latestDate = result.recordset[0].maxDate;
		const latestDateStr = latestDate.toISOString().split("T")[0];
		console.log(latestDateStr);
		return latestDateStr;
	} catch (err) {
		if (retryCount > 3) {
			return err;
		}
		console.log(err);
		console.log(
			"error occured in fetching maxDate from RawData table,retrying retryCount: " +
				retryCount
		);
		await new Promise((resolve) => setTimeout(resolve, delay));
		try {
			pool.close();
		} catch (err) {
			console.log(err);
		}
		findMaxDate(retryCount + 1, delay);
	} finally {
		if (pool) {
			console.log("connection to db closed");
			pool.close();
		}
	}
}

function getDatesFromStartDate(date) {
	const startDate = new Date(date);
	const currentDate = new Date();
	const dates = [];
	// Loop through dates from start date to current date
	for (let d = startDate; d <= currentDate; d.setDate(d.getDate() + 1)) {
		dates.push(new Date(d));
	}
	return dates;
}

async function checkIfAllFilesExist(startDate) {
	try {
		const files = fs.readdirSync(downloadDir);
		const fileDates = files
			.map((fileName) => getDateFromFileName(fileName))
			.filter((date) => date !== null);
		const holidaysFromDB = await getHolidaysFromDB(startDate);
		const comprehensiveHolidayList = getComprehensiveHolidayList(
			holidaysFromDB,
			startDate
		);
		const validDates = getValidDates(startDate, comprehensiveHolidayList);
		if (!checkIfValidDatesFilesAvailable(fileDates, validDates)) {
			return false;
		}
		return true;
	} catch (err) {
		console.error(err);
	}
}

function checkIfValidDatesFilesAvailable(fileDates, validDates) {
	const fileDatesSet = new Set(fileDates);
	const validDatesSet = new Set(validDates);
	for (let date of validDatesSet) {
		if (!fileDatesSet.has(date)) {
			return false;
		}
	}
	return true;
}

function getValidDates(startDate, comprehensiveHolidayList) {
	const validDates = [];
	const currentDate = new Date();
	// Loop through each day from startDate to currentDate
	for (
		let date = new Date(startDate);
		date <= currentDate;
		date.setDate(date.getDate() + 1)
	) {
		const dateString = date.toISOString().split("T")[0];
		if (!comprehensiveHolidayList.includes(dateString)) {
			validDates.push(dateString);
		}
	}
	return validDates;
}

function getComprehensiveHolidayList(holidaysFromDB, startDate) {
	const currentDate = new Date().toISOString().slice(0, 10);
	const allDates = [];
	let currentDateObj = new Date(startDate);
	while (currentDateObj <= new Date(currentDate)) {
		const currentDateStr = currentDateObj.toISOString().slice(0, 10);
		// If the current date is a Saturday or Sunday and it's not already in the
		// holidayList, add it to the allDates list.
		const currentDayOfWeek = currentDateObj.getDay();
		if (
			(currentDayOfWeek === 6 || currentDayOfWeek === 0) &&
			!holidaysFromDB.includes(currentDateStr)
		) {
			countOfSaturdays++;
			allDates.push(currentDateStr);
		}
		// If the current date is in the holidayList, add it to the allDates list.
		if (holidaysFromDB.includes(currentDateStr)) {
			allDates.push(currentDateStr);
		}
		currentDateObj.setDate(currentDateObj.getDate() + 1);
	}
	return allDates;
}

async function getHolidaysFromDB(startDate) {
	try {
		let pool = await new ConnectionPool(conf).connect();
		const l_query =
			"SELECT Hdate FROM Holidays WHERE Hdate >= '" +
			startDate +
			"' AND Hdate <= GETDATE()";
		console.log(l_query);
		console.log(pool);
		const result = await pool.request().query(l_query);
		// console.log("this is resultSet : " + result.recordset);
		const holidays = result.recordset.map(
			(record) => record.Hdate.toISOString().split("T")[0]
		);
		// console.log("holidays from db are : " + holidays);
		return holidays;
	} catch (err) {
		console.error("Error occured while getting holidays from database : ", err);
		throw err;
	}
}

module.exports = {
	findMaxDate,
	checkIfAllFilesExist,
	downloadSpotData,
	downloadSpotDataFNF,
};
