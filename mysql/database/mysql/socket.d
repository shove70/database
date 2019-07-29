module database.mysql.socket;

import core.stdc.errno;

import std.socket;
import std.exception;
import std.datetime;

import database.mysql.exception;

struct Socket
{
    void connect(const(char)[] host, ushort port)
    {
        socket_ = new TcpSocket();
        socket_.connect(new InternetAddress(host, port));
        socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.KEEPALIVE, true);
        socket_.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
        socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, 30.seconds);
        socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, 30.seconds);
    }

    bool connected() inout
    {
        return socket_ && socket_.isAlive();
    }

    void close()
    {
        if (socket_) 
        {
            socket_.shutdown(SocketShutdown.BOTH);
            socket_.close();
            socket_ = null;
        }
    }

    void read(ubyte[] buffer)
    {
        long len;

        for (size_t off; off < buffer.length; off += len)
        {
            len = socket_.receive(buffer[off..$]);

            if (len > 0)
            {
                continue;
            }

            if (len == 0)
            {
                throw new MySQLConnectionException("Server closed the connection");
            }

            if ((errno == EINTR) || (errno == EAGAIN) || (errno == EWOULDBLOCK))
            {
                len = 0;

                continue;
            }

            throw new MySQLConnectionException("Received std.socket.Socket.ERROR: " ~ formatSocketError(errno));
        }
    }

    void write(in ubyte[] buffer)
    {
        long len;

        for (size_t off; off < buffer.length; off += len)
        {
            len = socket_.send(buffer[off..$]);

            if (len > 0)
            {
                continue;
            }

            if (len == 0)
            {
                throw new MySQLConnectionException("Server closed the connection");
            }

            if ((errno == EINTR) || (errno == EAGAIN) || (errno == EWOULDBLOCK))
            {
                len = 0;

                continue;
            }

            throw new MySQLConnectionException("Sent std.socket.Socket.ERROR: " ~ formatSocketError(errno));
        }
    }

private:

    TcpSocket socket_;
}
