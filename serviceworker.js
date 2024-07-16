const textDecoder = new TextDecoder('utf-8');
const textEncoder = new TextEncoder();

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
  const rangeStart = partSize * (partIndex - 1);
  const rangeEnd = rangeStart + 1;
  const headers = new Headers({
    'x-backblaze-live-read-enabled': 'true',
    'range': `bytes=${rangeStart}-${rangeEnd}`,
  });
  const response = await fetch(url, {
    headers: headers,
    mode: 'cors'
  });
  const exists = response.ok;
  void response.body.cancel();

  return exists
}

const getLastPartNumber = async ( url ) => {
  // Ugh - binary search - just for now!
  let upperBound = 10000;
  const partSize = await getPartSize(url);

  let highPart= upperBound;
  let lowPart= 1;

  if (!await partExists(url, partSize, lowPart)) {
    // The file has no parts
    return 0;
  }

  // Loop invariants: lowPart exists, highPart does not exist
  // Terminate loop when lowPart + 1 == highPart
  while (highPart - lowPart > 1) {
    let midPart = Math.floor(lowPart + (highPart - lowPart) / 2);
    console.log(`low: ${lowPart}, mid: ${midPart}, high: ${highPart}`);
    if (await partExists(url, partSize, midPart)) {
      lowPart = midPart;
    } else {
      highPart = midPart;
    }
  }

  return lowPart;
};

const lastPartCache = {};

const getCachedLastPartNumber = async ( url ) => {
  if (!lastPartCache[url]) {
    lastPartCache[url] = await getLastPartNumber(url);
  }

  return lastPartCache[url];
};

// https://stackoverflow.com/a/14163193/33905
Uint8Array.prototype.indexOfMulti = function(searchElements, fromIndex) {
  fromIndex = fromIndex || 0;

  let index = Array.prototype.indexOf.call(this, searchElements[0], fromIndex);
  if(searchElements.length === 1 || index === -1) {
    // Not found or no other elements to check
    return index;
  }

  while (index < this.length) {
    let i, j;
    for(i = index, j = 0; j < searchElements.length && i < this.length; i++, j++) {
      if(this[i] !== searchElements[j]) {
        break;
      }
    }

    if (i === index + searchElements.length) {
      return index;
    } else {
      index++;
    }
  }

  return -1;
};

const promises = {};

const UINT_SIZE = 4;
const BOX_HEADER_SIZE = 2 * UINT_SIZE;

// Adapted from https://stackoverflow.com/a/54312392/33905
function getUint32(arr, index = 0) { // From bytes to big-endian 32-bit integer.  Input: Uint8Array, index
  const dv = new DataView(arr.buffer, 0);
  return dv.getUint32(index, false); // big endian
}

function getUint64(arr, index = 0) { // From bytes to big-endian 32-bit integer.  Input: Uint8Array, index
  const dv = new DataView(arr.buffer, 0);
  return dv.getBigUint64(index, false); // big endian
}

function setUint32(arr, index, value) {
  const dv = new DataView(arr.buffer, 0);
  dv.setUint32(index, value);
}

function setUint64(arr, index, value) {
  const dv = new DataView(arr.buffer, 0);
  dv.setBigUint64(index, value);
}

function toString(arr, fr, to) { // From bytes to string.  Input: Uint8Array, start index, stop index.
  return String.fromCharCode.apply(null, arr.slice(fr,to));
}

function getAtom(arr, i) { // input Uint8Array, start index
  return [getUint32(arr, i), toString(arr, i+4, i+8)]
}

function getSubAtomOffsetLen(arr, box_name, offset = 0) { // input Uint8Array, box name
  const [main_length, ] = getAtom(arr, offset);
  offset += BOX_HEADER_SIZE;

  while (offset < main_length) {
    const [len, name] = getAtom(arr, offset);

    if (box_name === name) {
      return {offset, len};
    }
    offset += len;
  }
  return null;
}

function getSubAtom(arr, box_name) { // input Uint8Array, box name
  const offsetLen = getSubAtomOffsetLen(arr, box_name);
  return offsetLen ? arr.slice(offsetLen.offset, offsetLen.offset + offsetLen.len) : null;
}

function atomName(buffer, offset = 0) {
  const str = buffer.slice(offset + UINT_SIZE, offset + (UINT_SIZE * 2));
  try {
    return textDecoder.decode(str);
  } catch (e) {
    console.log(`Error decoding atom name: {e}`);
    return null;
  }
}

function isAtom(buffer, offset, name) {
  return atomName(buffer, offset) === name;
}

// Look for the moof structure in a buffer of MP4 data:
// [moof] size=i
//     [mfhd] size=j
//     - data
//     [traf] size=k
function getMoof(buffer) {
  let found = false;
  let start = 0;
  while (!found) {
    const moofIndex = buffer.indexOfMulti(textEncoder.encode('moof'), start) - UINT_SIZE;

    if (moofIndex > 0) {
      const mfhdIndex = moofIndex + BOX_HEADER_SIZE;
      if (isAtom(buffer, mfhdIndex, 'mfhd')) {
        const mfhdAtomLen = getUint32(buffer, mfhdIndex);
        if (isAtom(buffer, mfhdIndex + mfhdAtomLen, 'traf')) {
          const moofLen = getUint32(buffer, moofIndex);
          return {
            offset: moofIndex,
            len: moofLen,
            buffer: buffer.slice(moofIndex, moofIndex + moofLen)
          };
        }
      }
    }

    start = moofIndex + 4;
  }

  return null;
}

const getMoofSequenceNumber = ( buffer ) => {
  // Sequence number is in mfhd
  const mfhd = getSubAtom(buffer, 'mfhd');

  // mfhd is first atom in moof and has structure
  // Version        8 bits = 1 byte
  // Flags          24 bits = 3 bytes
  // SequenceNumber 32 bits = 4 bytes

  return getUint32(mfhd, BOX_HEADER_SIZE + UINT_SIZE);
}

const getMoofBaseDataOffset  = ( moof ) => {
  // BaseDataOffset is in traf/tfhd
  const traf = getSubAtom(moof, 'traf');
  const tfhd = getSubAtom(traf, 'tfhd');

  // traf is second atom in moof
  // tfhd is first atom in traf and has structure
  // Version        8 bits = 1 byte
  // Flags          24 bits = 3 bytes
  // TrackId        32 bits = 4 bytes
  // BaseDataOffset 64 bits = 8 bytes
  // ...

  return getUint64(tfhd, BOX_HEADER_SIZE + (UINT_SIZE * 2));
}

const getMoofBaseMediaDecodeTimes = ( moof ) => {
  // BaseMediaDecodeTime is in traf/tfdt
  const traf = getSubAtomOffsetLen(moof, 'traf');
  let tfdt = getSubAtomOffsetLen(moof, 'tfdt', traf.offset);

  // traf is second atom in moof
  // tfdt is second atom in traf and has structure
  // Version             8 bits = 1 byte
  // Flags               24 bits = 3 bytes
  // BaseMediaDecodeTime 64 bits = 8 bytes

  // Get time for each track
  const baseMediaDecodeTimes = [];
  baseMediaDecodeTimes.push(getUint64(moof, tfdt.offset + BOX_HEADER_SIZE + UINT_SIZE));
  traf.offset += traf.len;
  tfdt = getSubAtomOffsetLen(moof, 'tfdt', traf.offset);
  baseMediaDecodeTimes.push(getUint64(moof, tfdt.offset + BOX_HEADER_SIZE + UINT_SIZE));

  return baseMediaDecodeTimes;
}

const setMoofSequenceNumber = ( moof, sequenceNumber ) => {
  // Sequence number is in mfhd
  const mfhd = getSubAtomOffsetLen(moof, 'mfhd');

  // mfhd has structure
  // Version        8 bits = 1 byte
  // Flags          24 bits = 3 bytes
  // SequenceNumber 32 bits = 4 bytes

  setUint32(moof, mfhd.offset + BOX_HEADER_SIZE + UINT_SIZE, sequenceNumber);
}

const setMoofBaseDataOffset  = ( moof, baseDataOffset, tracks ) => {
  // BaseDataOffset is in traf/tfhd
  const traf = getSubAtomOffsetLen(moof, 'traf');

  // traf is second atom in moof
  // tfhd is first atom in traf and has structure
  // Version        8 bits = 1 byte
  // Flags          24 bits = 3 bytes
  // TrackId        32 bits = 4 bytes
  // BaseDataOffset 64 bits = 8 bytes
  // ...
  let trafOffset = traf.offset;
  for (let track = 0; track < tracks; track++) {
    const trafLen = getUint32(moof, trafOffset);
    const tfhd = getSubAtomOffsetLen(moof, 'tfhd', trafOffset);
    setUint64(moof, tfhd.offset + BOX_HEADER_SIZE + (UINT_SIZE * 2), baseDataOffset);
    trafOffset += trafLen;
  }
}

const setMoofBaseMediaDecodeTimes = ( moof, baseMediaDecodeTimes, tracks ) => {
  // BaseMediaDecodeTime is in traf/tfdt
  const traf = getSubAtomOffsetLen(moof, 'traf');

  // traf is second atom in moof
  // tfdt is second atom in traf and has structure
  // Version             8 bits = 1 byte
  // Flags               24 bits = 3 bytes
  // BaseMediaDecodeTime 64 bits = 8 bytes

  // Set time for each track
  let trafOffset = traf.offset;
  for (let track = 0; track < tracks; track++) {
    const trafLen = getUint32(moof, trafOffset);
    const tfdt = getSubAtomOffsetLen(moof, 'tfdt', trafOffset);
    setUint64(moof, tfdt.offset + BOX_HEADER_SIZE + UINT_SIZE, baseMediaDecodeTimes[track]);
    trafOffset += trafLen;
  }
}

class LargeFileAtomStream {
  #partNumber; // Next part number to read
  #buffer = null;
  #offset = 0; // Current offset into #buffer
  #interval = 1000;
  #stopped = false;

  constructor(url, startPartNumber) {
    this.url = url;
    this.#partNumber = startPartNumber;
  }

  get partNumber() {
    return this.#partNumber;
  }

  get offset() {
    return this.#offset;
  }

  async #initBufferIfNecessary() {
    if (!this.#buffer) {
      await this.#fetchPart();
    }
  }

  // Fetch a part, retrying if it is not available
  async #fetchPartByNumber(partNumber) {
    while (!this.#stopped) {
      const partUrl = this.url + `?partNumber=${partNumber}`;
      console.log(`Fetching ${partUrl}`);
      const response = await fetch(partUrl, {
        headers: {
          'x-backblaze-live-read-enabled': 'true'
        },
        mode: 'cors', // Have to use cors or same-origin to set custom headers
      });
      console.log('Received response:');
      console.log(response);
      if (response.ok) {
        return new Uint8Array(await response.arrayBuffer());
      } else {
        // TBD - be more discriminating about errors
        console.log(response.status, await response.text());
        await new Promise(r => setTimeout(r, this.#interval));
      }
    }
  }

  // Fetch the current part
  async #fetchPart() {
    const bufferRemaining = this.#buffer ? this.#buffer.byteLength - this.#offset : 0;
    const responseBuffer = await this.#fetchPartByNumber(this.#partNumber);
    if (this.#buffer) {
      const newBuffer = new Uint8Array(bufferRemaining + responseBuffer.byteLength);
      newBuffer.set(this.#buffer.slice(this.#offset, this.#offset + bufferRemaining));
      newBuffer.set(responseBuffer, bufferRemaining);
      this.#buffer = newBuffer;
    } else {
      this.#buffer = responseBuffer;
    }
    this.#offset = 0;
    this.#partNumber++;
  }

  async getMp4HeaderAndFirstMoof() {
    const firstPart = await this.#fetchPartByNumber(1);

    if (!isAtom(firstPart, 0, 'ftyp')) {
      return null;
    }
    const ftypLen = getUint32(firstPart, 0);
    if (!isAtom(firstPart, ftypLen, 'moov')) {
      return null;
    }
    const moovLen = getUint32(firstPart, ftypLen);

    const mp4HeaderLen = ftypLen + moovLen;

    const [moofLen, atomName] = getAtom(firstPart, mp4HeaderLen);
    if (atomName !== 'moof') {
      return null;
    }

    const mp4Header = firstPart.slice(0, mp4HeaderLen);
    const firstMoof = firstPart.slice(mp4HeaderLen, mp4HeaderLen + moofLen);

    return [mp4Header, firstMoof];
  }


  async peek(len){
    await this.#initBufferIfNecessary();

    const bufferRemaining = this.#buffer.byteLength - this.#offset;
    if (len > bufferRemaining) {
      await this.#fetchPart();
    }
    return this.#buffer.slice(this.#offset, this.#offset + len);
  }

  async read(len){
    const data = await this.peek(len);
    this.#offset += len;
    return data;
  }

  async getNextMoof() {
    await this.#initBufferIfNecessary();

    let moof = getMoof(this.#buffer);
    if (!moof) {
      // moof header may straddle the part boundary, so fast-forward to just before the end of the current part
      this.#offset = this.#buffer.byteLength - 8;
      await this.#fetchPart();
      moof = getMoof(this.#buffer);
      if (!moof) {
        // Don't get any more parts - just give up!
        return null;
      }
    }
    this.#offset = moof.offset + moof.len;

    return moof.buffer;
  }

  async getNextAtom() {
    const atomLen = getUint32(await this.peek(UINT_SIZE));
    return this.read(atomLen);
  }
}

const myFetch = async ( request ) => {
  if (request.destination === 'video') {
  // if (request.headers.get('Content-Type') === 'video/mp4') {
    console.log('Processing request:');
    console.log(request);
    for (const header of request.headers) {
      console.log(header);
    }

    const url = request.url;

    // Don't interleave requests for the last part!
    if (promises[url]) {
      await promises[url];
    }
    promises[url] = getCachedLastPartNumber(url);

    const lastPart = await promises[url];
    console.log(`${url} is ${lastPart} parts long`);

    // Leave a little buffer
    let currentPart = lastPart - 1;
    let responseSize = 0;

    // Values from first moof in the file
    let firstSequenceNumber;
    let firstBaseDataOffset;
    let firstBaseMediaDecodeTimes = [];

    // Values from first moof in the stream
    let startingSequenceNumber;
    let startingBaseDataOffset;
    let startingBaseMediaDecodeTimes = [];

    const atomStream = new LargeFileAtomStream(url, currentPart);

    const stream = new ReadableStream({
      async start(controller) {
        // Return a promise so the element knows to wait on it
        return new Promise( async (resolve, reject) => {
          console.log('Starting');
          try {
            const [mp4Header, firstMoof] = await atomStream.getMp4HeaderAndFirstMoof();

            // Get values from the first moof in the file
            firstSequenceNumber = getMoofSequenceNumber(firstMoof);
            firstBaseDataOffset = getMoofBaseDataOffset(firstMoof);
            firstBaseMediaDecodeTimes = getMoofBaseMediaDecodeTimes(firstMoof);
            console.log(`Found first sequence number: ${firstSequenceNumber}`)
            console.log(`Found first base data offset: ${firstBaseDataOffset}`)
            console.log(`Found first base media decode times: ${firstBaseMediaDecodeTimes}`)

            // Find the next moof starting from the current file part
            const moof = await atomStream.getNextMoof();
            if (!moof) {
              controller.error(`Cannot find moof atom in ${url}, part number ${atomStream.partNumber}`);
              return;
            }
            console.log(`Found moof at: ${atomStream.offset}`);

            startingSequenceNumber = getMoofSequenceNumber(moof);
            startingBaseDataOffset = getMoofBaseDataOffset(moof);
            startingBaseMediaDecodeTimes = getMoofBaseMediaDecodeTimes(moof);
            console.log(`Found starting sequence number: ${startingSequenceNumber}`)
            console.log(`Found starting base data offset: ${startingBaseDataOffset}`)
            console.log(`Found starting base media decode times: ${startingBaseMediaDecodeTimes}`)

            // Simply copy the values from the first moof in the file
            setMoofSequenceNumber(moof, firstSequenceNumber);
            setMoofBaseDataOffset(moof, firstBaseDataOffset, 2); // TBD - get number of tracks from moov
            setMoofBaseMediaDecodeTimes(moof, firstBaseMediaDecodeTimes, 2);

            // Get the mdat from after the moof
            const mdat = await atomStream.getNextAtom();
            if (!isAtom(mdat, 0, 'mdat')) {
              const message = `Unexpected atom: ${atomName(mdat, 0)}`;
              console.log(message)
              controller.error(message);
              return;
            }

            // Enqueue the header, moof and mdat
            controller.enqueue(mp4Header);
            controller.enqueue(moof);
            controller.enqueue(mdat);
            resolve();
          } catch (e) {
            console.log(e);
            controller.error(e);
            reject();
          }
        });
      },
      async pull(controller) {
        console.log('Pulling');
        const moof = await atomStream.getNextAtom();
        if (!isAtom(moof, 0, 'moof')) {
          controller.error(`Unexpected atom: ${atomName(moof)}`);
          return;
        }

        let sequenceNumber = getMoofSequenceNumber(moof);
        let baseDataOffset = getMoofBaseDataOffset(moof);
        let baseMediaDecodeTimes = getMoofBaseMediaDecodeTimes(moof);
        console.log(`Found sequence number: ${sequenceNumber}`)
        console.log(`Found base data offset: ${baseDataOffset}`)
        console.log(`Found base media decode times: ${baseMediaDecodeTimes}`)

        // Adjust the numbers
        sequenceNumber = (sequenceNumber - startingSequenceNumber) + firstSequenceNumber;
        baseDataOffset = (baseDataOffset - startingBaseDataOffset) + firstBaseDataOffset;
        for (let i = 0; i < baseMediaDecodeTimes.length; i++) {
          baseMediaDecodeTimes[i] = (baseMediaDecodeTimes[i] - startingBaseMediaDecodeTimes[i]) + firstBaseMediaDecodeTimes[i];
        }
        console.log(`Adjusted sequence number: ${sequenceNumber}`)
        console.log(`Adjusted base data offset: ${baseDataOffset}`)
        console.log(`Adjusted base media decode times: ${baseMediaDecodeTimes}`)

        setMoofSequenceNumber(moof, sequenceNumber);
        setMoofBaseDataOffset(moof, baseDataOffset, 2);
        setMoofBaseMediaDecodeTimes(moof, baseMediaDecodeTimes, 2);

        let checkSequenceNumber = getMoofSequenceNumber(moof);
        let checkBaseDataOffset = getMoofBaseDataOffset(moof);
        let checkBaseMediaDecodeTimes = getMoofBaseMediaDecodeTimes(moof);
        console.log(`Check sequence number: ${checkSequenceNumber}`)
        console.log(`Check base data offset: ${checkBaseDataOffset}`)
        console.log(`Check base media decode times: ${checkBaseMediaDecodeTimes}`)

        controller.enqueue(moof);

        // Get the mdat from after the moof
        const mdat = await atomStream.getNextAtom();
        if (!isAtom(mdat, 0, 'mdat')) {
          controller.error(`Unexpected atom: ${atomName(mdat, 0)}`);
          return;
        }
        controller.enqueue(mdat);

        console.log('Leaving pull()');
      },
      cancel(reason) {
        console.log(`Cancelling because ${reason} - enqueued a total of ${responseSize} bytes`);
      },
    });

    console.log('Returning chunked response');
    // Make a new chunked response that streams back parts
    return new Response(stream, {
      headers: {
        'Transfer-Encoding': 'chunked',
        'Content-Type': 'video/mp4'
      }
    });
  } else {
    console.log('Forwarding request:');
    console.log(request);
  }

  return fetch(request);
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
