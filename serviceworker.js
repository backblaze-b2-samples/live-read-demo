const getPartSize = async ( url ) => {
  const headers = new Headers({
    'x-backblaze-live-read-enabled': 'true',
    'range': 'bytes=0-1',
  });
  const response = await fetch(url, {
    headers: headers,
    mode: 'cors',
  });
  return response.headers.get('x-backblaze-live-read-part-size');
};

const partExists = async ( url, partSize, partIndex ) => {
  const rangeStart = partSize * partIndex;
  const rangeEnd = rangeStart + 1;
  const headers = new Headers({
    'x-backblaze-live-read-enabled': 'true',
    'range': `bytes=${rangeStart}-${rangeEnd}`,
  });
  const response = await fetch(url, {
    headers: headers,
    mode: 'cors'
  });
  return response.ok;
}

const getCurrentFileSize = async ( url ) => {
  const partSize = await getPartSize(url);
  console.log(`Part size is ${partSize}`);
  if (!partSize) {
    return 0;
  }

  // Ugh - binary search - just for now!
  let upperBoundBytes = 5 * 1024 * 1024 * 1024; // 5 GB
  let highPart = Math.ceil(upperBoundBytes / partSize);
  let lowPart = 1; // We got a size, so part 0 exists

  if (await partExists(url, partSize, highPart)) {
    // Oh well; the file is bigger than our upper bound
    return highPart * partSize;
  }

  if (!await partExists(url, partSize, lowPart)) {
    // The file is one part long
    return 1;
  }

  // Loop invariants: lowPart exists, highPart does not exist
  // Terminate loop when lowPart + 1 == highPart
  while (lowPart + 1 < highPart) {
    let midPart = Math.floor(lowPart + (highPart - lowPart) / 2);
    console.log(`low: ${lowPart}, mid: ${midPart}, high: ${highPart}`);
    if (await partExists(url, partSize, midPart)) {
      lowPart = midPart;
    } else {
      highPart = midPart;
    }
  }

  return (lowPart + 1) * partSize;
};

const fileSizeCache = {};
const cacheTimeout = 5 * 1000;

const getCachedFileSize = async ( url ) => {
  let fileSize
  if (fileSizeCache[url] && (Date.now() - fileSizeCache[url].timestamp < cacheTimeout)) {
    fileSize = fileSizeCache[url].size;
  } else {
    fileSize = await getCurrentFileSize(url);
    fileSizeCache[url] = {
      size: fileSize,
      timestamp: Date.now()
    }
  }
  return fileSize;
};

const promises = {};

const myFetch = async ( request ) => {
  let modified = false;
  if (request.destination === 'video') {
    const url = request.url;
    // Don't interleave requests for the file size!
    if (promises[url]) {
      await promises[url];
    }
    promises[url] = getCachedFileSize(url);
    const fileSize = await promises[url];
    console.log(`${url} is ${fileSize} bytes long`);

    console.log(`Incoming range: ${request.headers.get('range')}`)
    const headers = new Headers(request.headers);
    headers.set('x-backblaze-live-read-enabled', 'true');

    let range = headers.get('range') || 'bytes=0-';
    const regex = /^bytes=([0-9]+)?-([0-9]+)?$/
    const found = range.match(regex);
    const rangeStart = (found.length === 3) ? found[1] : null;
    let rangeEnd = (found.length === 3) ? found[2] : null;
    if (!rangeEnd) {
      rangeEnd = (fileSize - 1).toString();
    }
    range = `bytes=${rangeStart}-${rangeEnd}`
    headers.set('range', range)

    request = new Request(request, {
      headers: headers,
      mode: 'cors', // Have to use cors or same-origin to set custom headers
    });
    console.log(`Outgoing range: ${request.headers.get('range')}`)
    modified = true;
  }

  let response = await fetch(request);
  console.log(response);

  if (modified && response.ok) {
    let contentRange = response.headers.get('Content-Range')
    if (contentRange) {
      console.log(`Incoming content range: ${contentRange}`);
      let rangePart, sizePart;
      [rangePart, sizePart] = contentRange.split('/');
      // sizePart = (parseInt(sizePart) + 5242880).toString();
      sizePart = '*';
      contentRange =  `${rangePart}/${sizePart}`;

      const headers = new Headers(response.headers);
      headers.set('Content-Range', contentRange);

      // headers property of response is immutable and read only, so we have to make a new Response object
      response = new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: headers
      })

      contentRange = response.headers.get('Content-Range')
      console.log(`Outgoing content range: ${contentRange}`)
    }
  }
  return response;
};

self.addEventListener("install", (_event) => {
  void self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", event => {
  event.respondWith(myFetch(event.request));
});
