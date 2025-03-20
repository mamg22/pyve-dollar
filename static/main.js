const dollar = document.getElementById("USD");
const bolivar = document.getElementById("VED");
const date = document.getElementById("date");
const source = document.getElementById("source");
const currentRate = document.getElementById("current-rate");
const main = document.getElementsByTagName("main")[0];

let rate = 0;

function setInputState(enable) {
    const inputs = main.querySelectorAll("input, select");

    for (const input of inputs) {
        if (input.id != "date") {
            input.disabled = enable;
        }
    }
}

let currentAbortController = null;

async function fetchRate() {
    if (currentAbortController !== null) {
        currentAbortController.abort()
    }

    currentAbortController = new AbortController();

    const params = new URLSearchParams();
    params.append("source", source.value);
    params.append("value", 10000);
    if (date.value) {
        params.append("date", date.value);
    }

    const url = document.location.origin + "/api/v1?" + params;

    try {
        setInputState(true);
        const response = await fetch(url, { signal: currentAbortController.signal });
        if (!response.ok) {
            throw new Error(`Bad response status: ${response.status}`);
        }

        const value = await response.text();

        rate = value;

        document.dispatchEvent(new CustomEvent("rateUpdated"));
    }
    catch (err) {
        console.log(err.message);
    }
    finally {
        setInputState(false);
    }
}

function updateFromVED() {
    ved = bolivar.value;
    dollar.value = (ved / rate * 10000).toFixed(4);
}

function updateFromUSD() {
    usd = dollar.value;
    bolivar.value = (usd * rate / 10000).toFixed(4);
}

function updateDisplayedRate() {
    currentRate.innerText = `Bs. ${(rate / 10000).toFixed(4)}`;
}

dollar.addEventListener("input", updateFromUSD);
bolivar.addEventListener("input", updateFromVED);
date.addEventListener("change", fetchRate);
source.addEventListener("change", fetchRate);

document.addEventListener("rateUpdated", updateDisplayedRate);
document.addEventListener("rateUpdated", updateFromUSD);

fetchRate();
