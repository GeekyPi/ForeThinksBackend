const {
	findMaxDate,
	downloadSpotData,
	downloadSpotDataFNF,
} = require("./utils/BhavcopyDownloaderUtil");
const {spawn} = require("child_process");
const {insertBhavCopyIntoDB} = require("./utils/BhavcopyInsertionUtil");

async function executeTask() {
	const now = new Date();
	const scheduleTime = new Date(
		now.getFullYear(),
		now.getMonth(),
		now.getDate(),
		0,
		0,
		0
	); // Schedule time is 8:00 PM
	let timeRemaining = scheduleTime.getTime() - now.getTime();
	try {
		if (timeRemaining < 0) {
			setTimeout(() => {
				setInterval(downloadBhavcopy, 24 * 60 * 60 * 100);
				downloadBhavcopy();
			}, 24 * 60 * 60 * 1000 + timeRemaining);
			downloadBhavcopy();
		} else {
			setTimeout(async function scheduledTask() {
				setInterval(downloadBhavcopy, 24 * 60 * 60 * 1000);
				downloadBhavcopy();
			}, timeRemaining);
		}
	} catch (err) {
		return err;
	}
}

async function downloadBhavcopy(retryCount = 1, delay = 30 * 60 * 1000) {
	const downloaderScriptPath = "./scripts/BhavCopyDownloader.py";
	const currentDate = new Date();
	const currentDateInFormat = currentDate.toISOString().split("T")[0];
	try {
		// finds the latest date from raw data table
		let startDate = await findMaxDate();
		let nextDay;
		if (startDate === undefined) {
			startDate = "2019-01-01";
			nextDay = new Date(startDate);
		} else {
			nextDay = new Date(startDate);
			nextDay.setDate(nextDay.getDate() + 1);
		}
		nextDay = nextDay.toISOString().slice(0, 10);
		// runs the BhavCopyDownloader.py script file
		await new Promise((resolve, reject) => {
			const process = spawn("python", [
				downloaderScriptPath,
				nextDay,
				currentDateInFormat,
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
					await downloadSpotData();
					await downloadSpotDataFNF();
					insertBhavCopyIntoDB(startDate);
					resolve();
				}
			});
		});
		console.log("promise above resolved");
	} catch (err) {
		if (retryCount > 12) {
			return err;
		}
		console.log(
			"error occured in downloadBhavCopy, retry count : " +
				retryCount +
				"errosMsg : " +
				err
		);
		await new Promise((resolve) => setTimeout(resolve, delay));
		downloadBhavcopy(retryCount++, delay);
	}
}

module.exports = {
	executeTask,
};
