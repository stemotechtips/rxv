import asyncio
import socket
import struct
import aiohttp
from ssdp import RxvDetails


async def discover_notify(timeout=60, local_ip="192.168.1.100"):
    """Listen asynchronously for YamahaRemoteControl SSDP NOTIFY packets."""

    multicast_addr = "239.255.255.250"
    port = 1900

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM,
        socket.IPPROTO_UDP,
    )

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((local_ip, port))

    membership = struct.pack(
        "4s4s",
        socket.inet_aton(multicast_addr),
        socket.inet_aton(local_ip),
    )

    sock.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_ADD_MEMBERSHIP,
        membership,
    )

    sock.setblocking(False)

    loop = asyncio.get_running_loop()
    responses = []
    start_time = loop.time()

    try:
        while (loop.time() - start_time) < timeout:
            remaining = timeout - (loop.time() - start_time)

            try:
                data, addr = await asyncio.wait_for(
                    loop.sock_recvfrom(sock, 65535),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                break

            message = data.decode("utf-8", errors="replace")

            if not message.startswith("NOTIFY * HTTP/1.1"):
                continue

            headers = {}

            for line in message.split("\r\n")[1:]:
                if ":" not in line:
                    continue

                key, value = line.split(":", 1)
                headers[key.strip().upper()] = value.strip()

            # Only accept YamahaRemoteControl notifications
            if "YamahaRemoteControl" not in headers.get("NT", ""):
                continue

            # Must have a LOCATION
            if "LOCATION" not in headers:
                continue

            responses.append(headers)

            print("\n--- SSDP packet received ---")
            print(f"Source: {addr[0]}:{addr[1]}")
            print(f"NT: {headers.get('NT')}")
            print(f"LOCATION: {headers.get('LOCATION')}")

    finally:
        sock.close()

    return responses


import xml.etree.ElementTree as ET


def parse_device_xml(xml):
    root = ET.fromstring(xml)

    namespace = {
        "upnp": "urn:schemas-upnp-org:device-1-0"
    }

    device = root.find("upnp:device", namespace)

    ctrl_url = device.findtext("upnp:presentationURL", namespaces=namespace) + "YamahaRemoteControl/ctrl"

    #print(device.findtext("upnp:presentationURL", namespaces=namespace))
    #print(device.findtext("upnp:modelName", namespaces=namespace))
    #print(device.findtext("upnp:friendlyName", namespaces=namespace))
    #print(device.findtext("upnp:serialNumber", namespaces=namespace))

    return RxvDetails(ctrl_url, "None", device.findtext("upnp:modelName", namespaces=namespace), device.findtext("upnp:friendlyName", namespaces=namespace), device.findtext("upnp:serialNumber", namespaces=namespace))



async def get_RxvDetails(responses):

    xml = None
    receivers = []

    async with aiohttp.ClientSession() as session:


        for response in responses:

            location = response.get('LOCATION')

            print(f"\nGetting XML from: {location}")

            try:
                async with session.get(location) as result:
                    result.raise_for_status()

                    xml = await result.text()

                    #print(xml)
                    receivers.append(parse_device_xml(xml))
                    

            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                print(f"Failed to get XML: {error}")

        return receivers


async def get_legacy_devices():

    responses = await discover_notify(
        timeout=30,
        local_ip="192.168.1.11",
    )

    print(f"\nFound {len(responses)} Yamaha devices")

    return await get_RxvDetails(responses)


if __name__ == "__main__":
    asyncio.run(get_legacy_devices())