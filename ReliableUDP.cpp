#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>

#include "Net.h"

using namespace std;
using namespace net;

const int ServerPort = 30000;
const int ClientPort = 30001;
const int ProtocolId = 0x11223344;
const float TimeOut = 10.0f;

const int ChunkDataSize = 200;

enum PacketType : unsigned char
{
    PT_INFO = 1,
    PT_DATA = 2,
    PT_ACK = 3,
    PT_DONE = 4,
    PT_DONE_ACK = 5
};

void WriteU32(unsigned char* buf, uint32_t value)
{
    buf[0] = (unsigned char)(value >> 24);
    buf[1] = (unsigned char)(value >> 16);
    buf[2] = (unsigned char)(value >> 8);
    buf[3] = (unsigned char)(value);
}

uint32_t ReadU32(const unsigned char* buf)
{
    return (uint32_t(buf[0]) << 24) | (uint32_t(buf[1]) << 16) |
        (uint32_t(buf[2]) << 8) | (uint32_t(buf[3]));
}

void WriteU16(unsigned char* buf, uint16_t value)
{
    buf[0] = (unsigned char)(value >> 8);
    buf[1] = (unsigned char)(value);
}

uint16_t ReadU16(const unsigned char* buf)
{
    return (uint16_t(buf[0]) << 8) | (uint16_t(buf[1]));
}

bool SendReliable(ReliableConnection& connection, unsigned char* packet, int size,
    unsigned char expectedAckType, uint32_t expectedAckValue)
{
    const int maxRetries = 30;
    const float retryTimeout = 0.2f;
    const float pollInterval = 0.01f;

    for (int attempt = 0; attempt < maxRetries; attempt++)
    {
        connection.SendPacket(packet, size);

        float waited = 0.0f;
        while (waited < retryTimeout)
        {
            unsigned char reply[256];
            int bytes = connection.ReceivePacket(reply, sizeof(reply));
            if (bytes >= 5 && reply[0] == expectedAckType)
            {
                uint32_t value = ReadU32(&reply[1]);
                if (value == expectedAckValue)
                    return true;
            }
            net::wait(pollInterval);
            waited += pollInterval;
        }
    }
    return false;
}

void SendAck(ReliableConnection& connection, unsigned char type, uint32_t value)
{
    unsigned char ack[5];
    ack[0] = type;
    WriteU32(&ack[1], value);
    connection.SendPacket(ack, sizeof(ack));
}

bool SendFile(ReliableConnection& connection, const string& filePath)
{
    ifstream file(filePath, ios::binary | ios::ate);
    if (!file.is_open())
    {
        printf("failed to open file: %s\n", filePath.c_str());
        return false;
    }

    streamsize fileSize = file.tellg();
    file.seekg(0, ios::beg);

    vector<unsigned char> fileData((size_t)fileSize);
    if (fileSize > 0 && !file.read(reinterpret_cast<char*>(fileData.data()), fileSize))
    {
        printf("failed to read file\n");
        return false;
    }
    file.close();

    size_t slash = filePath.find_last_of("/\\");
    string fileName = (slash == string::npos) ? filePath : filePath.substr(slash + 1);
    if (fileName.size() > 200) fileName = fileName.substr(0, 200);

    printf("sending file: %s (%zu bytes)\n",
        fileName.c_str(), fileData.size());

    {
        unsigned char packet[256];
        int offset = 0;
        packet[offset++] = PT_INFO;
        packet[offset++] = (unsigned char)fileName.size();
        memcpy(&packet[offset], fileName.data(), fileName.size());
        offset += (int)fileName.size();
        WriteU32(&packet[offset], (uint32_t)fileData.size());
        offset += 4;

        if (!SendReliable(connection, packet, offset, PT_ACK, 0xFFFFFFFF))
        {
            printf("no response from receiver for file info, aborting\n");
            return false;
        }
    }

    uint32_t totalChunks = (uint32_t)((fileData.size() + ChunkDataSize - 1) / ChunkDataSize);
    if (totalChunks == 0) totalChunks = 1;

    for (uint32_t chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
    {
        size_t offsetInFile = (size_t)chunkIndex * ChunkDataSize;
        size_t remaining = fileData.size() - offsetInFile;
        uint16_t thisChunkSize = (uint16_t)min((size_t)ChunkDataSize, remaining);

        unsigned char packet[256];
        int offset = 0;
        packet[offset++] = PT_DATA;
        WriteU32(&packet[offset], chunkIndex);
        offset += 4;
        WriteU16(&packet[offset], thisChunkSize);
        offset += 2;
        memcpy(&packet[offset], &fileData[offsetInFile], thisChunkSize);
        offset += thisChunkSize;

        if (!SendReliable(connection, packet, offset, PT_ACK, chunkIndex))
        {
            printf("receiver stopped responding at chunk %u, aborting\n", chunkIndex);
            return false;
        }

        if (chunkIndex % 50 == 0)
            printf("sent chunk %u / %u\n", chunkIndex, totalChunks);
    }

    {
        unsigned char packet[5];
        packet[0] = PT_DONE;
        WriteU32(&packet[1], totalChunks);
        if (!SendReliable(connection, packet, sizeof(packet), PT_DONE_ACK, totalChunks))
        {
            printf("receiver never confirmed DONE\n");
            return false;
        }
    }

    printf("\ntransfer complete\n");

    return true;
}

void ReceiveFiles(ReliableConnection& connection)
{
    string outputFileName;
    uint32_t expectedFileSize = 0;
    vector<unsigned char> receivedData;
    bool receivingFile = false;

    printf("waiting for a file...\n");

    while (true)
    {
        unsigned char packet[256];
        int bytes = connection.ReceivePacket(packet, sizeof(packet));

        if (bytes <= 0)
        {
            net::wait(0.01f);
            continue;
        }

        unsigned char type = packet[0];

        if (type == PT_INFO)
        {
            int offset = 1;
            unsigned char nameLen = packet[offset++];
            outputFileName = "received_" + string(reinterpret_cast<char*>(&packet[offset]), nameLen);
            offset += nameLen;
            expectedFileSize = ReadU32(&packet[offset]);
            offset += 4;

            receivedData.clear();
            receivedData.resize(expectedFileSize);
            receivingFile = true;

            printf("\nincoming file: %s (%u bytes)\n",
                outputFileName.c_str(), expectedFileSize);

            SendAck(connection, PT_ACK, 0xFFFFFFFF);
        }
        else if (type == PT_DATA && receivingFile)
        {
            uint32_t chunkIndex = ReadU32(&packet[1]);
            uint16_t dataLen = ReadU16(&packet[5]);
            size_t offsetInFile = (size_t)chunkIndex * ChunkDataSize;

            if (offsetInFile + dataLen <= receivedData.size())
                memcpy(&receivedData[offsetInFile], &packet[7], dataLen);

            if (chunkIndex % 50 == 0)
                printf("received chunk %u\n", chunkIndex);

            SendAck(connection, PT_ACK, chunkIndex);
        }
        else if (type == PT_DONE && receivingFile)
        {
            uint32_t totalChunks = ReadU32(&packet[1]);

            ofstream out(outputFileName, ios::binary);
            out.write(reinterpret_cast<char*>(receivedData.data()), receivedData.size());
            out.close();

            printf("\nfile received: %s\n", outputFileName.c_str());
            printf("\n");

            SendAck(connection, PT_DONE_ACK, totalChunks);

            receivingFile = false;
            printf("waiting for a file...\n");
        }
    }
}

int main(int argc, char* argv[])
{
    enum Mode { Client, Server };
    Mode mode = Server;
    Address address;
    string filePath;

    // parse command line

    if (argc >= 3)
    {
        int a, b, c, d;
#pragma warning(suppress : 4996)
        if (sscanf(argv[1], "%d.%d.%d.%d", &a, &b, &c, &d))
        {
            mode = Client;
            address = Address(a, b, c, d, ServerPort);
            filePath = argv[2];
        }
    }

    // initialize

    if (!InitializeSockets())
    {
        printf("failed to initialize sockets\n");
        return 1;
    }

    ReliableConnection connection(ProtocolId, TimeOut);

    const int port = (mode == Server) ? ServerPort : ClientPort;

    if (!connection.Start(port))
    {
        printf("could not start connection on port %d\n", port);
        return 1;
    }

    if (mode == Client)
    {
        connection.Connect(address);
        bool ok = SendFile(connection, filePath);
        if (!ok)
            printf("file transfer FAILED\n");
    }
    else
    {
        connection.Listen();
        ReceiveFiles(connection);
    }

    ShutdownSockets();
    return 0;
}