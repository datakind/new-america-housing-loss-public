    // A Function that helps convert image data that is in memory
    function encode (input) {
    var keyStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    var output = "";
    var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
    var i = 0;

    while (i < input.length) {
        chr1 = input[i++];
        chr2 = i < input.length ? input[i++] : Number.NaN; // Not sure if the index 
        chr3 = i < input.length ? input[i++] : Number.NaN; // checks are needed here

        enc1 = chr1 >> 2;
        enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
        enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
        enc4 = chr3 & 63;

        if (isNaN(chr2)) {
            enc3 = enc4 = 64;
        } else if (isNaN(chr3)) {
            enc4 = 64;
        }
        output += keyStr.charAt(enc1) + keyStr.charAt(enc2) +
                  keyStr.charAt(enc3) + keyStr.charAt(enc4);
    }
    return output;
}

// Drag and drop code that is use to allow drag and drop
    let dropArea = document.querySelector('.droparea');
    ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, preventDefaults, false)
    })

    function preventDefaults(e) {
        e.preventDefault()
        e.stopPropagation()
    }

    const highlight = () => dropArea.classList.add("green-border");
    const unhighlight = () => dropArea.classList.remove("green-border");

    ;['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, highlight, false)
    })

    ;['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, unhighlight, false)
    })

    dropArea.addEventListener("drop", handleDrop, false);

    function handleDrop(e) {
        let dt = e.dataTransfer
        let files = dt.files
        handleFiles(files);
    }

    // A function that handles the uploading of the file
    function handleFiles(files) {
        loadtimegui = document.getElementById("loadtime")
        loggerphotoid = document.getElementById("loggerphoto")
        loggerphotoid.innerHTML = ""
        csvdivid = document.getElementById("csvdiv")
        csvdivid.innerHTML = ""
        csvtimeid = document.getElementById("csvtime")
        csvtimeid.innerHTML = ""
        loadtimetext = document.createElement('p')
        loadtimetext.innerHTML = "Uploading csv file... (Please wait 10-20 seconds)"
        loadtimetext.style.cssText += 'float: left;'
        loadicon = document.createElement("div")
        loadicon.classList.add("dots-bars-4")
        loadicon.setAttribute("id","loadiconid")
        loadicon.style.cssText += 'margin-left: auto;margin-right: auto;'
        loadtimegui.appendChild(loadtimetext)
        loadtimegui.appendChild(loadicon);
        {
        ([...files]).forEach(uploadFile)}
    }
    async function uploadFile(file) {
        const url = '/'
        let formData = new FormData()
        console.log(file);
        formData.append("file", file);

        let responseOptions = {
            method: 'POST',
            files: file,
            body: formData
        }
        console.log(responseOptions);

        await fetch(url, responseOptions)
            .then(() => { console.log('Finished'); })
            .catch(() => { /* Error. Inform the user */ })
    }

    // A function that converts a nested object array into csv for downloading
    function ConvertToCSV(objArray,csvheader) {
            var array = typeof objArray != 'object' ? JSON.parse(objArray) : objArray;
            var str = '';
            var headerline = ""
            for (var i = 0; i < csvheader.length; i++) {
                
                console.log(csvheader[i])
	        if (headerline != '') headerline += ','
            headerline += csvheader[i];
            console.log(headerline)
                }
            str += headerline + '\r\n';
            for (var i = 0; i < array.length; i++) {
                var line = '';
                for (var index in array[i]) {
                    if (line != '') line += ','

                    line += array[i][index];
                }

                str += line + '\r\n';
            }

            return str;
        }

    // Websocket initialization
    var socket = io();

    $('#test').click(function(msg){
        console.log(msg.test)
    })
    // runForecase websocket code
    $('#runTool').click(function(event) {
                socket.emit('run_tool');
                
                return false;
            })

    socket.on('loadicon',function(msg){
            loadtimegui = document.getElementById("loadtime")
            loadtimegui.innerHTML = ""
            loggerphotoid = document.getElementById("loggerphoto")
            loggerphotoid.innerHTML = ""
            csvdivid = document.getElementById("csvdiv")
            csvdivid.innerHTML = ""
            csvtimeid = document.getElementById("csvtime")
            csvtimeid.innerHTML = ""
            loadtimetext = document.createElement('p')
            loadtimetext.innerHTML = "Preparing Realtime calculation.... (Please wait 10-20 seconds)"
            loadtimetext.style.cssText += 'float: left;'
            loadtimetext.setAttribute("id","loadtimetextid")
            loadicon = document.createElement("div")
            loadicon.classList.add("dots-bars-4")
            loadicon.setAttribute("id","loadiconid")
            loadicon.style.cssText += 'margin-left: auto;margin-right: auto;'
            loadtimegui.appendChild(loadtimetext)
            loadtimegui.appendChild(loadicon)
    })
    // Websocket to show zip data on screen
    socket.on('showzip', function(msg) {
        fetch("/sendFile")
        .then((res) => { 
            console.log(res)
            return res.blob(); })
          .then((data) => {
            console.log(data)

            loadtimeid = document.getElementById("csvtime")
            loadtimegui = document.getElementById("loadtime")
            loadtimegui.innerHTML = ""
            loadtimeid.innerHTML = ""
            csvdownloadlink = document.createElement("a")
            csvdownloadlink.innerHTML = "Download Zip File"
            csvdownloadlink.href = window.URL.createObjectURL(data);
            csvdownloadlink.download = "results.zip";

            loadtimeid.append(csvdownloadlink)
            socket.emit('killdata');
          })
    }),

    // A websocket for different error catches
    socket.on('logerror', function(msg){

            document.getElementById("loadtime").innerHTML = ""
            document.getElementById("output").innerHTML = ""
            $('#output').append(msg.loginfo)

    })

        // A websocket for different error catches
        socket.on('toolAR', function(msg){

            loadtimetext = document.getElementById("loadtimetextid")
            loadtimetext.innerHTML = msg.loginfo
            loadtimetext.style.cssText += "font-weight: bolder;font-size: 16;"

    })

    // A websocket to clear output of the logger on screen
    socket.on('clearoutput', function(msg) {
        document.getElementById("output").innerHTML = ""
    })

    // The websocket that pipes the runtime interpreter to the screen
    socket.on('logTool', function(msg) {
        console.log('logTool')
        console.log(msg.loginfo)
        $('#output').append(msg.loginfo)
        }
    )

    // A websocket to start the photo and show it on screen
    socket.on('toolphoto', function(msg) {
        console.log(msg.loginfo)
        var arrayBuffer = msg.loginfo;
        var bytes = new Uint8Array(arrayBuffer);
        loggerphotocontainer = document.getElementById("loggerphoto")
        
        loggerphoto = document.createElement("img")
        loggerphoto.setAttribute("src",'data:image/png;base64,'+encode(bytes))
        loggerphoto.setAttribute("alt","Forecast result")
        loggerphoto.style.cssText += "margin-left: auto;margin-right: auto;"
        loggerphotocontainer.appendChild(loggerphoto)
        loadtimeid = document.getElementById("loadtime")
        loadtimeid.innerHTML = "CHART DATA BELOW - CLICK HERE TO DOWNLOAD"
        loadtimeid.href = 'data:image/png;base64,'+encode(bytes)
        loadtimeid.download = 'chartdata.png';
        socket.emit('killdata');

    })

    // A websocket to let the user know the upload is completed
    socket.on('uploadcomplete', function(msg) {
        console.log('upload is complete')
        document.getElementById("output").innerHTML = ""
        document.getElementById("loadtime").innerHTML = "" 
        loggerphotoid = document.getElementById("loggerphoto")
        loggerphotoid.innerHTML = ""
        csvdivid = document.getElementById("csvdiv")
        csvdivid.innerHTML = ""
        csvtimeid = document.getElementById("csvtime")
        csvtimeid.innerHTML = ""
        loadtimetext = document.createElement('p')
        loadtimetext.innerHTML = "CSV upload completed"
        loadtimetext.style.cssText += 'float: left;'
        loadtimegui.appendChild(loadtimetext)

    })