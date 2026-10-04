/*
 * FILE          : ReliableUDP.cpp
 * PROJECT       : Assignment 1
 * PROGRAMMER    : Aliaksandr Kazeika , Halvin Silva Mayes
 * DESCRIPTION   :
 * program that transfers files over UDP using a custom
   protocol on top of ReliableConnection, verifies them with a whole-file
   CRC32 check, and reports transfer time and speed in Mbps.
 */
 /* Reliability and Flow Control Example
From "Networking for Game Programmers" - http://www.gaffer.org/networking-for-game-programmers
Author: Glenn Fiedler gaffer@gaffer.org
								 */
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>

#include "Net.h"

								 //#define SHOW_ACKS

using namespace std;
using namespace net;

//default ports, can be changed with --port and --client-port
const int ServerPort = 30000;
const int ClientPort = 30001;
const int ProtocolId = 0x11223344;
const float DeltaTime = 1.0f / 30.0f;
const float SendRate = 1.0f / 30.0f;
const float TimeOut = 10.0f;
const int PacketSize = 256;

class FlowControl
{
public:

	FlowControl()
	{
		printf("flow control initialized\n");
		Reset();
	}

	void Reset()
	{
		mode = Bad;
		penalty_time = 4.0f;
		good_conditions_time = 0.0f;
		penalty_reduction_accumulator = 0.0f;
	}

	void Update(float deltaTime, float rtt)
	{
		const float RTT_Threshold = 250.0f;

		if (mode == Good)
		{
			if (rtt > RTT_Threshold)
			{
				printf("*** dropping to bad mode ***\n");
				mode = Bad;
				if (good_conditions_time < 10.0f && penalty_time < 60.0f)
				{
					penalty_time *= 2.0f;
					if (penalty_time > 60.0f)
						penalty_time = 60.0f;
					printf("penalty time increased to %.1f\n", penalty_time);
				}
				good_conditions_time = 0.0f;
				penalty_reduction_accumulator = 0.0f;
				return;
			}

			good_conditions_time += deltaTime;
			penalty_reduction_accumulator += deltaTime;

			if (penalty_reduction_accumulator > 10.0f && penalty_time > 1.0f)
			{
				penalty_time /= 2.0f;
				if (penalty_time < 1.0f)
					penalty_time = 1.0f;
				printf("penalty time reduced to %.1f\n", penalty_time);
				penalty_reduction_accumulator = 0.0f;
			}
		}

		if (mode == Bad)
		{
			if (rtt <= RTT_Threshold)
				good_conditions_time += deltaTime;
			else
				good_conditions_time = 0.0f;

			if (good_conditions_time > penalty_time)
			{
				printf("*** upgrading to good mode ***\n");
				good_conditions_time = 0.0f;
				penalty_reduction_accumulator = 0.0f;
				mode = Good;
				return;
			}
		}
	}

	float GetSendRate()
	{
		return mode == Good ? 30.0f : 10.0f;
	}

private:

	enum Mode
	{
		Good,
		Bad
	};

	Mode mode;
	float penalty_time;
	float good_conditions_time;
	float penalty_reduction_accumulator;
};

//our file-transfer packet types (sent on top of ReliableConnection)
enum PacketType : unsigned char
{
	PT_INFO = 1,
	PT_DATA = 2,
	PT_STATUS = 3,
	PT_DONE = 4
};

//flags in STATUS packet
const unsigned char STATUS_HAVE_INFO = 1;
const unsigned char STATUS_COMPLETE = 2;
const unsigned char STATUS_CRC_OK = 4;

const int StatusSize = 10;
const int DataHeaderSize = 7;
//max file bytes per DATA packet
const int ChunkDataSize = PacketSize - DataHeaderSize;
//how many chunks can be sent before going back to the first missing one
const uint32_t WindowSize = 32;

//CRC32 lookup table (standard polynomial 0xEDB88320), based on RFC 1952 sample code
unsigned int crc32_table[256];

void BuildCrc32Table()
{
	for (unsigned int i = 0; i < 256; i++)
	{
		unsigned int c = i;
		for (int j = 0; j < 8; j++)
			c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
		crc32_table[i] = c;
	}
}

//calculates CRC32 of the whole buffer
unsigned int Crc32(const unsigned char* data, size_t length)
{
	unsigned int crc = 0xFFFFFFFF;
	for (size_t i = 0; i < length; i++)
		crc = crc32_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
	return crc ^ 0xFFFFFFFF;
}

//helpers to write/read integers into byte buffers
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
		(uint32_t(buf[2]) << 8) | uint32_t(buf[3]);
}

void WriteU16(unsigned char* buf, uint16_t value)
{
	buf[0] = (unsigned char)(value >> 8);
	buf[1] = (unsigned char)(value);
}

uint16_t ReadU16(const unsigned char* buf)
{
	return (uint16_t)((buf[0] << 8) | buf[1]);
}

double SecondsSince(chrono::high_resolution_clock::time_point start)
{
	return chrono::duration<double>(chrono::high_resolution_clock::now() - start).count();
}

//client side state
struct FileSender
{
	vector<unsigned char> data;
	string fileName;
	uint32_t fileCrc = 0;
	uint32_t transferId = 0;
	uint32_t totalChunks = 0;
	uint32_t ackedChunks = 0;
	uint32_t nextChunk = 0;
	bool infoAcked = false;
	bool complete = false;
	bool crcOk = false;
	chrono::high_resolution_clock::time_point startTime;
};

//server side state
struct FileReceiver
{
	bool haveInfo = false;
	bool complete = false;
	bool crcOk = false;
	uint32_t transferId = 0;
	string outputFileName;
	uint32_t fileSize = 0;
	uint32_t expectedCrc = 0;
	uint32_t totalChunks = 0;
	uint32_t nextMissing = 0;
	vector<unsigned char> data;
	vector<bool> received;
	chrono::high_resolution_clock::time_point startTime;
};

//load whole file into memory and calculate its crc32
bool LoadFile(FileSender& sender, const string& filePath)
{
	ifstream file(filePath, ios::binary | ios::ate);
	if (!file.is_open())
	{
		printf("failed to open file: %s\n", filePath.c_str());
		return false;
	}

	streamsize fileSize = file.tellg();
	file.seekg(0, ios::beg);

	sender.data.resize((size_t)fileSize);
	if (fileSize > 0 && !file.read(reinterpret_cast<char*>(sender.data.data()), fileSize))
	{
		printf("failed to read file: %s\n", filePath.c_str());
		return false;
	}

	//keep only file name no folder path
	size_t slash = filePath.find_last_of("/\\");
	sender.fileName = (slash == string::npos) ? filePath : filePath.substr(slash + 1);
	if (sender.fileName.size() > 200)
		sender.fileName = sender.fileName.substr(0, 200);

	sender.fileCrc = Crc32(sender.data.data(), sender.data.size());
	sender.totalChunks = (uint32_t)((sender.data.size() + ChunkDataSize - 1) / ChunkDataSize);
	sender.transferId = (uint32_t)chrono::high_resolution_clock::now().time_since_epoch().count();
	if (sender.transferId == 0)
		sender.transferId = 1;

	printf("sending file: %s (%zu bytes, %u chunks), crc32 = 0x%08X\n",
		sender.fileName.c_str(), sender.data.size(), sender.totalChunks, sender.fileCrc);
	return true;
}

//builds the next packet to send: INFO, then DATA chunks, then DONE
int BuildSenderPacket(FileSender& sender, unsigned char* packet)
{
	int offset = 0;

	//Info packet
	if (!sender.infoAcked)
	{
		packet[offset++] = PT_INFO;
		WriteU32(&packet[offset], sender.transferId);
		offset += 4;
		packet[offset++] = (unsigned char)sender.fileName.size();
		memcpy(&packet[offset], sender.fileName.data(), sender.fileName.size());
		offset += (int)sender.fileName.size();
		WriteU32(&packet[offset], (uint32_t)sender.data.size());
		offset += 4;
		WriteU32(&packet[offset], sender.fileCrc);
		offset += 4;
		return offset;
	}

	//Data packets
	if (sender.ackedChunks < sender.totalChunks)
	{
		//go back to the first missing chunk when the window is used up
		uint32_t windowEnd = min(sender.totalChunks, sender.ackedChunks + WindowSize);
		if (sender.nextChunk < sender.ackedChunks || sender.nextChunk >= windowEnd)
			sender.nextChunk = sender.ackedChunks;

		uint32_t chunkIndex = sender.nextChunk++;
		size_t fileOffset = (size_t)chunkIndex * ChunkDataSize;
		uint16_t length = (uint16_t)min((size_t)ChunkDataSize, sender.data.size() - fileOffset);

		packet[offset++] = PT_DATA;
		WriteU32(&packet[offset], chunkIndex);
		offset += 4;
		WriteU16(&packet[offset], length);
		offset += 2;
		memcpy(&packet[offset], &sender.data[fileOffset], length);
		offset += length;
		return offset;
	}

	//tells receiver the file is complete
	packet[offset++] = PT_DONE;
	WriteU32(&packet[offset], sender.transferId);
	offset += 4;
	return offset;
}

//client reads STATUS from the server (works as ACK)
void HandleStatus(FileSender& sender, const unsigned char* packet, int bytes)
{
	if (bytes < StatusSize || packet[0] != PT_STATUS)
		return;

	unsigned char flags = packet[1];
	uint32_t transferId = ReadU32(&packet[2]);
	uint32_t nextMissing = ReadU32(&packet[6]);

	if (transferId != sender.transferId)
		return;

	if (flags & STATUS_HAVE_INFO)
		sender.infoAcked = true;

	if (nextMissing > sender.ackedChunks && nextMissing <= sender.totalChunks)
	{
		if (nextMissing / 50 != sender.ackedChunks / 50 || nextMissing == sender.totalChunks)
			printf("confirmed chunks: %u / %u\n", nextMissing, sender.totalChunks);
		sender.ackedChunks = nextMissing;
	}

	if ((flags & STATUS_COMPLETE) && sender.ackedChunks == sender.totalChunks)
	{
		sender.complete = true;
		sender.crcOk = (flags & STATUS_CRC_OK) != 0;
	}
}

//server builds STATUS packet: what it already received
int BuildStatusPacket(const FileReceiver& receiver, unsigned char* packet)
{
	unsigned char flags = 0;
	if (receiver.haveInfo)
		flags |= STATUS_HAVE_INFO;
	if (receiver.complete)
		flags |= STATUS_COMPLETE;
	if (receiver.crcOk)
		flags |= STATUS_CRC_OK;

	packet[0] = PT_STATUS;
	packet[1] = flags;
	WriteU32(&packet[2], receiver.transferId);
	WriteU32(&packet[6], receiver.nextMissing);
	return StatusSize;
}

//checks crc32, saves file and prints time and speed
void FinishReceive(FileReceiver& receiver)
{
	double seconds = SecondsSince(receiver.startTime);
	double megabits = (receiver.data.size() * 8.0) / 1000000.0;
	unsigned int actualCrc = Crc32(receiver.data.data(), receiver.data.size());

	ofstream out(receiver.outputFileName, ios::binary);
	out.write(reinterpret_cast<const char*>(receiver.data.data()), receiver.data.size());
	out.close();

	receiver.complete = true;
	receiver.crcOk = (actualCrc == receiver.expectedCrc);

	printf("\nfile received: %s (%u bytes)\n", receiver.outputFileName.c_str(), receiver.fileSize);
	printf("time taken: %.3f seconds\n", seconds);
	printf("speed: %.3f Mbps\n", seconds > 0 ? megabits / seconds : 0.0);
	printf("expected crc32: 0x%08X\n", receiver.expectedCrc);
	printf("actual   crc32: 0x%08X\n", actualCrc);

	//compare checksums to detect corruption
	if (receiver.crcOk)
		printf("integrity check passed\n\n");
	else
		printf("integrity check failed, file is corrupted\n\n");

	printf("waiting for a file...\n");
}

//server side: handles INFO DATA DONE
void HandleReceiverPacket(FileReceiver& receiver, const unsigned char* packet, int bytes)
{
	unsigned char type = packet[0];

	if (type == PT_INFO && bytes >= 6)
	{
		uint32_t transferId = ReadU32(&packet[1]);

		//repeated INFO, keep data and timer
		if (receiver.haveInfo && transferId == receiver.transferId)
			return;

		int offset = 5;
		unsigned char nameLen = packet[offset++];
		if (offset + nameLen + 8 > bytes)
			return;

		receiver.outputFileName = "received_" + string((const char*)&packet[offset], nameLen);
		offset += nameLen;
		receiver.fileSize = ReadU32(&packet[offset]);
		offset += 4;
		receiver.expectedCrc = ReadU32(&packet[offset]);
		offset += 4;

		receiver.transferId = transferId;
		receiver.haveInfo = true;
		receiver.complete = false;
		receiver.crcOk = false;
		receiver.totalChunks = (uint32_t)(((uint64_t)receiver.fileSize + ChunkDataSize - 1) / ChunkDataSize);
		receiver.nextMissing = 0;
		receiver.data.assign(receiver.fileSize, 0);
		receiver.received.assign(receiver.totalChunks, false);
		receiver.startTime = chrono::high_resolution_clock::now();

		printf("\nincoming file: %s (%u bytes, %u chunks), expected crc32 = 0x%08X\n",
			receiver.outputFileName.c_str(), receiver.fileSize, receiver.totalChunks, receiver.expectedCrc);
	}
	else if (type == PT_DATA && receiver.haveInfo && !receiver.complete && bytes >= DataHeaderSize)
	{
		uint32_t chunkIndex = ReadU32(&packet[1]);
		uint16_t length = ReadU16(&packet[5]);
		size_t fileOffset = (size_t)chunkIndex * ChunkDataSize;

		if (chunkIndex >= receiver.totalChunks || DataHeaderSize + length > bytes ||
			fileOffset + length > receiver.data.size())
			return;

		//put chunk into its place, resent chunks are stored only once
		if (!receiver.received[chunkIndex])
		{
			memcpy(&receiver.data[fileOffset], &packet[DataHeaderSize], length);
			receiver.received[chunkIndex] = true;

			while (receiver.nextMissing < receiver.totalChunks && receiver.received[receiver.nextMissing])
				receiver.nextMissing++;

			if (chunkIndex % 50 == 0)
				printf("received chunk %u / %u\n", chunkIndex, receiver.totalChunks);
		}
	}
	else if (type == PT_DONE && receiver.haveInfo && !receiver.complete && bytes >= 5)
	{
		if (ReadU32(&packet[1]) == receiver.transferId && receiver.nextMissing == receiver.totalChunks)
			FinishReceive(receiver);
	}
}

void PrintUsage()
{
	printf("usage:\n");
	printf("  server: ReliableUDP.exe [--port <port>]\n");
	printf("  client: ReliableUDP.exe <server ip> <file> [--port <server port>] [--client-port <port>] [--corrupt]\n");
	printf("  defaults: server port %d, client port %d\n", ServerPort, ClientPort);
}

// ----------------------------------------------

int main(int argc, char* argv[])
{
	// parse command line

	enum Mode
	{
		Client,
		Server
	};

	Mode mode = Server;
	Address address;

	int serverPort = ServerPort;
	int clientPort = ClientPort;
	string filePath;
	bool corruptForTest = false;
	bool haveAddress = false;
	int a = 0, b = 0, c = 0, d = 0;

	for (int i = 1; i < argc; i++)
	{
		string arg = argv[i];

		if (arg == "--corrupt")
			corruptForTest = true;
		else if (arg == "--port" && i + 1 < argc)
			serverPort = atoi(argv[++i]);
		else if (arg == "--client-port" && i + 1 < argc)
			clientPort = atoi(argv[++i]);
		else if (!haveAddress)
		{
#pragma warning(suppress : 4996)
			if (sscanf(argv[i], "%d.%d.%d.%d", &a, &b, &c, &d) != 4)
			{
				printf("invalid server ip address: %s\n", argv[i]);
				PrintUsage();
				return 1;
			}
			haveAddress = true;
		}
		else if (filePath.empty())
			filePath = arg;
		else
		{
			printf("unknown argument: %s\n", argv[i]);
			PrintUsage();
			return 1;
		}
	}

	if (serverPort <= 0 || serverPort > 65535 || clientPort <= 0 || clientPort > 65535)
	{
		printf("port must be between 1 and 65535\n");
		return 1;
	}

	if (haveAddress)
	{
		mode = Client;
		address = Address(a, b, c, d, (unsigned short)serverPort);
	}

	FileSender sender;
	FileReceiver receiver;
	bool sendingFile = (mode == Client && !filePath.empty());

	if (sendingFile)
	{
		BuildCrc32Table();
		if (!LoadFile(sender, filePath))
			return 1;

		//corruption test: flip one byte after crc32 was calculated
		if (corruptForTest && !sender.data.empty())
		{
			size_t corruptOffset = sender.data.size() / 2;
			sender.data[corruptOffset] ^= 0xFF;
			printf("*** TEST: corrupting byte %zu (chunk %zu) on purpose to prove CRC check works ***\n",
				corruptOffset, corruptOffset / ChunkDataSize);
		}
	}
	else if (mode == Server)
	{
		BuildCrc32Table();
	}

	// initialize

	if (!InitializeSockets())
	{
		printf("failed to initialize sockets\n");
		return 1;
	}

	ReliableConnection connection(ProtocolId, TimeOut);

	const int port = mode == Server ? serverPort : clientPort;

	if (!connection.Start(port))
	{
		printf("could not start connection on port %d\n", port);
		return 1;
	}

	if (mode == Client)
		connection.Connect(address);
	else
	{
		connection.Listen();
		printf("waiting for a file...\n");
	}

	bool connected = false;
	float sendAccumulator = 0.0f;
	float statsAccumulator = 0.0f;

	FlowControl flowControl;

	//start timer
	sender.startTime = chrono::high_resolution_clock::now();

	while (true)
	{
		// update flow control

		if (connection.IsConnected())
			flowControl.Update(DeltaTime, connection.GetReliabilitySystem().GetRoundTripTime() * 1000.0f);

		const float sendRate = flowControl.GetSendRate();

		// detect changes in connection state

		if (mode == Server && connected && !connection.IsConnected())
		{
			flowControl.Reset();
			printf("reset flow control\n");
			connected = false;

			//client is gone, drop unfinished transfer
			if (receiver.haveInfo && !receiver.complete)
				printf("client disconnected, unfinished transfer dropped\n");
			receiver = FileReceiver();
		}

		if (!connected && connection.IsConnected())
		{
			printf("client connected to server\n");
			connected = true;
		}

		if (!connected && connection.ConnectFailed())
		{
			printf("connection failed\n");
			break;
		}

		if (mode == Client && connected && !connection.IsConnected())
		{
			printf("connection to server lost\n");
			break;
		}

		// send and receive packets

		sendAccumulator += DeltaTime;

		while (sendAccumulator > 1.0f / sendRate)
		{
			unsigned char packet[PacketSize];
			memset(packet, 0, sizeof(packet));
			int size = sizeof(packet);

			//server sends STATUS, client sends file packets
			if (mode == Server)
				size = BuildStatusPacket(receiver, packet);
			else if (sendingFile)
				size = BuildSenderPacket(sender, packet);

			connection.SendPacket(packet, size);
			sendAccumulator -= 1.0f / sendRate;
		}

		while (true)
		{
			unsigned char packet[256];
			int bytes_read = connection.ReceivePacket(packet, sizeof(packet));
			if (bytes_read == 0)
				break;

			if (mode == Server)
				HandleReceiverPacket(receiver, packet, bytes_read);
			else if (sendingFile)
				HandleStatus(sender, packet, bytes_read);
		}

		//client is done when server confirms the whole file
		if (sendingFile && sender.complete)
			break;

		// show packets that were acked this frame

#ifdef SHOW_ACKS
		unsigned int* acks = NULL;
		int ack_count = 0;
		connection.GetReliabilitySystem().GetAcks(&acks, ack_count);
		if (ack_count > 0)
		{
			printf("acks: %d", acks[0]);
			for (int i = 1; i < ack_count; ++i)
				printf(",%d", acks[i]);
			printf("\n");
		}
#endif

		// update connection

		connection.Update(DeltaTime);

		// show connection stats

		statsAccumulator += DeltaTime;

		while (statsAccumulator >= 0.25f && connection.IsConnected())
		{
			float rtt = connection.GetReliabilitySystem().GetRoundTripTime();

			unsigned int sent_packets = connection.GetReliabilitySystem().GetSentPackets();
			unsigned int acked_packets = connection.GetReliabilitySystem().GetAckedPackets();
			unsigned int lost_packets = connection.GetReliabilitySystem().GetLostPackets();

			float sent_bandwidth = connection.GetReliabilitySystem().GetSentBandwidth();
			float acked_bandwidth = connection.GetReliabilitySystem().GetAckedBandwidth();

			printf("rtt %.1fms, sent %d, acked %d, lost %d (%.1f%%), sent bandwidth = %.1fkbps, acked bandwidth = %.1fkbps\n",
				rtt * 1000.0f, sent_packets, acked_packets, lost_packets,
				sent_packets > 0.0f ? (float)lost_packets / (float)sent_packets * 100.0f : 0.0f,
				sent_bandwidth, acked_bandwidth);

			statsAccumulator -= 0.25f;
		}

		net::wait(DeltaTime);
	}

	//stop timer and calculate speed in Mbps
	int result = 0;
	if (sendingFile)
	{
		if (sender.complete)
		{
			double seconds = SecondsSince(sender.startTime);
			double megabits = (sender.data.size() * 8.0) / 1000000.0;

			printf("\ntransfer complete: %s (%zu bytes)\n", sender.fileName.c_str(), sender.data.size());
			printf("time taken: %.3f seconds\n", seconds);
			printf("speed: %.3f Mbps\n", seconds > 0 ? megabits / seconds : 0.0);
			printf("receiver integrity check: %s\n", sender.crcOk ? "passed" : "FAILED (file corrupted)");
		}
		else
		{
			printf("file transfer FAILED\n");
			result = 1;
		}
	}

	ShutdownSockets();

	return result;
}