#include <winsock2.h>
#include <ws2tcpip.h>
#include <cstdint>
#include <iostream>
#include <chrono>

#pragma comment(lib, "ws2_32.lib") // this links the WinSock Library

#pragma pack(push, 1) //pack the struct here no padding
struct IMUData {
    uint16_t magic;
    uint32_t t;
    float roll;
    float pitch;
    float yaw;
    float ax;
    float ay;
    float az;
}; //IMU data becomes 30 bytes since magic headers +2 bytes 

struct UDPPacket {
    IMUData imu;
    uint64_t mac_send_us;  // add the Mac time stamp to packet so +8 now 38 bytes
};
#pragma pack(pop)
//gets the current time right now
uint64_t now_us() {
    auto now = std::chrono::steady_clock::now();
    return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count();
}

int main() {
    WSADATA wsa; //start windows networking

    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::cout << "WSAStartup failed\n";
        return 1;
    }

    SOCKET sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP); // creates a UDP socket for receiving

    if (sock == INVALID_SOCKET) {
        std::cout << "Socket creation failed\n";
        return 1;
    }

    sockaddr_in server{}; // sets up lsitening address
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(5005); //listen on port 5005 on windows machine

    if (bind(sock, (sockaddr*)&server, sizeof(server)) == SOCKET_ERROR) { // binds the socket to port so anything listening on 5005 shoudl go to this program
        std::cout << "Bind failed\n";
        closesocket(sock);
        WSACleanup();
        return 1;
    }

    std::cout << "Listening on UDP port 5005...\n"; // prints startup info
    std::cout << "UDP packet size = " << sizeof(UDPPacket) << " bytes\n";

    uint64_t last_recv_us = 0; //used to calculate time in between received packets

    while (true) { //infinite receive loop waits for UDP packets forever
        UDPPacket packet{};

        sockaddr_in sender{};
        int senderSize = sizeof(sender);

        int bytes = recvfrom( //receive UDP packets
            sock,
            reinterpret_cast<char*>(&packet),
            sizeof(packet),
            0,
            (sockaddr*)&sender,
            &senderSize
        );
            //checks packet size must be 38 bytes if not then ignore it
        if (bytes != sizeof(UDPPacket)) {
            std::cout << "bad packet size: got bytes=" << bytes << "\n";
            continue;
        }

        if (packet.imu.magic != 0xAA55) { // checks magic header
            std::cout << "bad magic=0x"
                      << std::hex << packet.imu.magic << std::dec
                      << "\n";
            continue;
        }

       //calculates receive Interval
        uint64_t recv_us = now_us();

        double interval_ms = 0.0;
        if (last_recv_us != 0) {
            interval_ms = (recv_us - last_recv_us) / 1000.0;
        }
        last_recv_us = recv_us;

        //prints telemetry stats
        std::cout
            << "INTERVAL=" << interval_ms << " ms | "
            << "t=" << packet.imu.t << "ms | "
            << "R=" << packet.imu.roll
            << " P=" << packet.imu.pitch
            << " Y=" << packet.imu.yaw
            << " | ax=" << packet.imu.ax
            << " ay=" << packet.imu.ay
            << " az=" << packet.imu.az
            << '\n';
    }

    closesocket(sock);
    WSACleanup();
    return 0;
}
