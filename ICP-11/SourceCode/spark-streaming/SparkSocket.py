import socket
import sys
if __name__ == '__main__':

    file = open("D:\\UMKC\\__Spring2020\\CS5590BDP\\Module-2\\Lesson4\\spark-streaming\\lorem.txt", "r")
    filesss = file.readlines()

    HOST = 'localhost'
    PORT = 9999

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('# Socket created')

    # Create socket on port
    try:
        s.bind((HOST, PORT))
    except socket.error as msg:
        print('# Bind failed. ')
        sys.exit()

    print('# Socket bind complete')

    # Start listening on socket
    s.listen(10)
    print('# Socket now listening')

    # Wait for client
    conn, addr = s.accept()
    print('# Connected to ' + addr[0] + ':' + str(addr[1]))

    # Receive data from client
    while True:
        #data = conn.recv(1024)
        #line = data.decode('UTF-8')    # convert to string (Python 3 only)
        #line = line.replace("\n","")   # remove newline character
        for filestr in filesss:
            conn.send(str(filestr).encode('UTF-8'))

    s.close()