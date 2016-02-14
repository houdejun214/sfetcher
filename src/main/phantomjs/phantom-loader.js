"use strict";
var page = require('webpage').create();
var system = require('system');

var lastReceived = new Date().getTime();
var requestCount = 0;
var responseCount = 0;
var requestIds = [];
var startTime = new Date().getTime();

page.settings.loadImages=false;
page.settings.userAgent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36';
page.settings.javascriptEnabled = true;
page.settings.webSecurityEnabled = false;
page.settings.resourceTimeout = 10000;
page.onResourceReceived = function (response) {
    if(requestIds.indexOf(response.id) !== -1) {
        lastReceived = new Date().getTime();
        responseCount++;
        requestIds[requestIds.indexOf(response.id)] = null;
    }
};
page.onResourceRequested = function (requestData, request) {
    if ((/https?:\/\/.+?\.css/gi).test(requestData['url']) || requestData.headers['Content-Type'] == 'text/css') {
        //console.log('The url of the request is matching. Aborting: ' + requestData['url']);
        request.abort();
        return;
    }
    //console.log(requestData['url']);
    if(requestIds.indexOf(requestData.id) === -1) {
        requestIds.push(requestData.id);
        requestCount++;
    }
};

// Open the page
page.open(system.args[1],  function (status) {
     // Check for page load success
    if (status !== "success") {
        console.log(status);
        phantom.exit();
    } else {
        // Wait for all request finished
        waitFor();
    }
});

/**
 * Wait until the test condition is true or a timeout occurs. Useful for waiting
 * on a server response or for a ui change (fadeIn, etc.) to occur.
 *
 * @param timeOutMillis the max amount of time to wait. If not specified, 3 sec is used.
 */
function waitFor(timeOutMillis) {
    var maxtimeOutMillis = timeOutMillis ? timeOutMillis : 10000, //< Default Max Timout is 3s
        start = new Date().getTime(),
        condition = false,
        interval = setInterval(function() {
            if ((new Date().getTime() - start > maxtimeOutMillis) //wait timeout
                || ((new Date().getTime() - lastReceived)>500 && requestCount === responseCount)
             ) {
                console.log(page.content);
                clearInterval(interval);
                phantom.exit();
            }
        }, 100); //< repeat check every 100ms
};
