// Код для Cloudflare Pages: functions/[[path]].js

export async function onRequest(context) {
    const { request, env } = context;
    let userID = env.UUID || '12ffaeda-d4f0-4347-870d-a3b00dde30c2';
    let proxyIP = env.PROXYIP || '';

    try {
        const upgradeHeader = request.headers.get('Upgrade');
        if (!upgradeHeader || upgradeHeader !== 'websocket') {
            const url = new URL(request.url);
            switch (url.pathname) {
                case '/':
                    return new Response(JSON.stringify(request.cf), { status: 200 });
                case `/${userID}`: {
                    const vlessConfig = getVLESSConfig(userID, request.headers.get('Host'));
                    return new Response(`${vlessConfig}`, {
                        status: 200,
                        headers: { "Content-Type": "text/plain;charset=utf-8" }
                    });
                }
                default:
                    return new Response('Not found', { status: 404 });
            }
        } else {
            return await vlessOverWSHandler(request, userID, proxyIP);
        }
    } catch (err) {
        return new Response(err.toString());
    }
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (из твоего исходника) ---

async function vlessOverWSHandler(request, userID, proxyIP) {
    const webSocketPair = new WebSocketPair();
    const [client, webSocket] = Object.values(webSocketPair);
    webSocket.accept();

    let address = '';
    let portWithRandomLog = '';
    const log = (info, event) => console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
    const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
    const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

    let remoteSocketWapper = { value: null };
    let udpStreamWrite = null;
    let isDns = false;

    readableWebSocketStream.pipeTo(new WritableStream({
        async write(chunk, controller) {
            if (isDns && udpStreamWrite) return udpStreamWrite(chunk);
            if (remoteSocketWapper.value) {
                const writer = remoteSocketWapper.value.writable.getWriter();
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }
            const { hasError, message, portRemote = 443, addressRemote = '', rawDataIndex, vlessVersion = new Uint8Array([0, 0]), isUDP } = processVlessHeader(chunk, userID);
            address = addressRemote;
            portWithRandomLog = `${portRemote}--${Math.random()}`;
            if (hasError) throw new Error(message);
            if (isUDP) {
                if (portRemote === 53) isDns = true;
                else throw new Error('UDP only for DNS');
            }
            const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
            const rawClientData = chunk.slice(rawDataIndex);
            if (isDns) {
                const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
                udpStreamWrite = write;
                udpStreamWrite(rawClientData);
                return;
            }
            handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log, proxyIP);
        }
    })).catch((err) => log('pipeTo error', err));

    return new Response(null, { status: 101, webSocket: client });
}

// Вставь сюда остальные функции: handleTCPOutBound, makeReadableWebSocketStream, 
// processVlessHeader, remoteSocketToWS, base64ToArrayBuffer, handleUDPOutBound, 
// getVLESSConfig, safeCloseWebSocket, stringify (из твоего исходного файла)

function getVLESSConfig(userID, hostName) {
    const vlessMain = `vless://${userID}@${hostName}:443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#${hostName}`;
    return vlessMain; // Упростил для краткости
}
