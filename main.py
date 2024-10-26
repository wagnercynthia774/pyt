import bencodepy
import hashlib
import random
import requests
import socket
import struct
import os
import logging

total_downloaded = 0  # Total bytes downloaded

def parse_torrent(file_path):
    with open(file_path, 'rb') as f:
        return bencodepy.decode(f.read())

def extract_info(info):
    return {
        'name': info[b'name'].decode('utf-8'),
        'piece_length': info[b'piece length'],
        'pieces': info[b'pieces'],
        'files': [
            {
                'length': file[b'length'],
                'path': [part.decode('utf-8') for part in file[b'path']]
            }
            for file in info[b'files']
        ]
    }

def announce_to_tracker(tracker_url, info_hash, peer_id):
    params = {
        'info_hash': info_hash,
        'peer_id': peer_id,
        'port': 6881,
        'uploaded': 0,
        'downloaded': 0,
        'left': sum(file['length'] for file in extracted_info['files']),
        'event': 'started'
    }
    response = requests.get(tracker_url, params=params)
    return response.content

def generate_peer_id():
    return '-PY0001-' + ''.join(random.choices('0123456789abcdefghijklmnopqrstuvwxyz', k=12))

def parse_tracker_response(response):
    response_dict = bencodepy.decode(response)
    peers = response_dict[b'peers']

    peer_list = []
    for i in range(0, len(peers), 6):  # Each peer is 6 bytes (4 for IP, 2 for port)
        ip = '.'.join(map(str, peers[i:i + 4]))
        port = struct.unpack('>H', peers[i + 4:i + 6])[0]
        peer_list.append((ip, port))
    
    return peer_list

def connect_to_peer(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s

def download_piece(peer, piece_index, piece_length):
    total_size = sum(file['length'] for file in extracted_info['files'])  # Total size of all files
    global total_downloaded  # Use the global variable
    ip, port = peer

    # Connect to the peer
    s = connect_to_peer(ip, port)

    # Simulate downloading the piece
    downloaded_bytes = piece_length  # Assume full piece is downloaded for this example
    total_downloaded += downloaded_bytes

    # Calculate and print the download percentage
    percentage = (total_downloaded / total_size) * 100
    print(f"Downloaded piece {piece_index}, total downloaded: {total_downloaded} bytes ({percentage:.2f}%)")

    # Close the connection after downloading
    s.close()

def connect_to_udp_tracker(tracker_url, info_hash, peer_id):
    print(f"Connecting to UDP tracker at: {tracker_url}")
    
    # Parse the tracker URL
    scheme, host_port = tracker_url.split('://')
    host, port_path = host_port.split(':')
    port = int(port_path.split('/')[0])  # Extract port before the path

    print(f"Parsed host: {host}, port: {port}")

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect((host, port))

    # Send the connect request
    connection_id = 0x41727101980  # This is the magic connection ID for UDP trackers
    action = 0  # Connect
    transaction_id = random.randint(0, 0xFFFFFFFF)

    request = struct.pack('>Q', connection_id) + struct.pack('>I', action) + struct.pack('>I', transaction_id)
    print(f"Sending connect request: {request.hex()}")
    sock.send(request)

    # Receive the response
    response = sock.recv(16)
    action, transaction_id, connection_id = struct.unpack('>I I Q', response)
    print(f"Received response: action={action}, transaction_id={transaction_id}, connection_id={connection_id}")

    if action != 0:  # Check if the response is valid
        raise Exception("Invalid response from tracker")

    # Send an announce request
    action = 1  # Announce
    request = struct.pack('>Q', connection_id) + struct.pack('>I', action) + struct.pack('>I', transaction_id)
    request += info_hash + generate_peer_id().encode('utf-8') + struct.pack('>Q', 0) * 3  # 0s for uploaded, downloaded, left

    print(f"Sending announce request: {request.hex()}")
    sock.send(request)
    response = sock.recv(2048)

    # Process the announce response to get peers
    print(f"Received {len(response)} bytes from the tracker.")

    # Extract peers from the response
    peer_list = []
    peer_count = (len(response) - 20) // 6  # Each peer is 6 bytes (4 for IP, 2 for port)
    for i in range(peer_count):
        ip = '.'.join(map(str, response[20 + i * 6:20 + i * 6 + 4]))
        port = struct.unpack('>H', response[20 + i * 6 + 4:20 + i * 6 + 6])[0]
        peer_list.append((ip, port))

    sock.close()
    return peer_list

"""logic one"""
# logging.basicConfig(level=logging.DEBUG)
# # Parse the torrent file
# torrent_file = 'example.torrent'  # Change this to your torrent file path
# torrent_data = parse_torrent(torrent_file)

# # Extract the info dictionary
# info = torrent_data[b'info']
# extracted_info = extract_info(info)

# # Prepare to connect to the tracker
# tracker_url = torrent_data[b'announce'].decode('utf-8')
# info_hash = hashlib.sha1(bencodepy.encode(info)).digest()
# peer_id = generate_peer_id()

# # Announce to the tracker and get peers
# # tracker_response = announce_to_tracker(tracker_url, info_hash, peer_id)
# # peer_list = parse_tracker_response(tracker_response)

# # print(f"Peers: {peer_list}")

# peer_list = connect_to_udp_tracker(tracker_url, info_hash, peer_id)

# # Iterate over peers and download pieces
# for peer in peer_list:
#     for piece_index in range(len(extracted_info['pieces']) // 20):  # Adjust for total pieces
#         piece_length = extracted_info['piece_length']  # You might need to adjust this for the last piece
#         download_piece(peer, piece_index, piece_length)
if __name__ == "__main__":
    from ltorrent.client import Client

    torrent_path = "example.torrent"
    port = 8080

    client = Client(
        port=port
    )

    client.load(torrent_path=torrent_path)
    client.list_file()
    selection = input("Select file: ")
    client.select_file(selection=selection)
    client.run()