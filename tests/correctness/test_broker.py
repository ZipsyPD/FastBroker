import socket

HOST = "127.0.0.1"
PORT = 9090

def connect_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    s.connect((HOST, PORT))
    return s

def recv_lines(sock, count):
    data = ""

    while data.count("\n") < count:
        chunk = sock.recv(4096).decode()
        if not chunk:
            break
        data += chunk

    return data

def test_publish_subscribe():
    subscriber = connect_client()
    publisher = connect_client()

    subscriber.sendall(b"SUBSCRIBE sports\n")
    subscriber.recv(1024)  # consume confirmation

    publisher.sendall(b"PUBLISH sports hello\n")

    data = recv_lines(subscriber, 1)

    assert data == "MESSAGE sports hello\n"

    subscriber.close()
    publisher.close()

def test_replay():
    publisher = connect_client()
    client = connect_client()

    publisher.sendall(b"PUBLISH replay_test first\n")
    publisher.sendall(b"PUBLISH replay_test second\n")

    client.sendall(b"REPLAY replay_test 0\n")

    data = recv_lines(client, 2)

    assert "MESSAGE replay_test first\n" in data
    assert "MESSAGE replay_test second\n" in data

    publisher.close()
    client.close()

def test_ping():
    client = connect_client()

    client.sendall(b"PING\n")

    data = client.recv(1024).decode()

    assert data == "PONG\n"

    client.close()

def test_invalid_replay_offset():
    client = connect_client()

    client.sendall(b"REPLAY sports abc\n")

    data = recv_lines(client, 1)

    assert data == "Replay offset must be a number\n"

    client.close()

def test_topic_isolation():
    subscriber = connect_client()
    publisher = connect_client()

    subscriber.sendall(b"SUBSCRIBE sports\n")
    subscriber.recv(1024)

    publisher.sendall(b"PUBLISH games hello\n")

    subscriber.settimeout(0.5)

    try:
        subscriber.recv(1024)
        assert False, "Subscriber received a message from the wrong topic"
    except socket.timeout:
        pass

    subscriber.close()
    publisher.close()

def test_unknown_command():
    client = connect_client()

    client.sendall(b"HELLO\n")

    data = recv_lines(client, 1)

    assert "Unknown command" in data

    client.close()

def test_malformed_publish():
    client = connect_client()

    client.sendall(b"PUBLISH sports\n")

    data = recv_lines(client, 1)

    assert data == (
        "Incorrect usage of publish: should be "
        "(PUBLISH topic message)\n"
    )

    client.close()
