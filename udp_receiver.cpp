#include <winsock2.h>
#include <ws2tcpip.h>
#include <cstdint>
#include <iostream>
#include <chrono>

#pragma comment(lib, "ws2_32.lib")

#pragma pack(push, 1)
struct IMUData {
    uint16_t magic;
    uint32_t t;
    float roll;
    float pitch;
    float yaw;
    float ax;
    float ay;
    float az;
};

struct UDPPacket {
    IMUData imu;
    uint64_t mac_send_us;
};
#pragma pack(pop)

uint64_t now_us() {
    auto now = std::chrono::steady_clock::now();
    return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count();
}

int main() {
    WSADATA wsa;

    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::cout << "WSAStartup failed\n";
        return 1;
    }

    SOCKET sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (sock == INVALID_SOCKET) {
        std::cout << "Socket creation failed\n";
        return 1;
    }

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(5005);

    if (bind(sock, (sockaddr*)&server, sizeof(server)) == SOCKET_ERROR) {
        std::cout << "Bind failed\n";
        closesocket(sock);
        WSACleanup();
        return 1;
    }

    std::cout << "Listening on UDP port 5005...\n";
    std::cout << "UDP packet size = " << sizeof(UDPPacket) << " bytes\n";

    uint64_t last_recv_us = 0;

    while (true) {
        UDPPacket packet{};

        sockaddr_in sender{};
        int senderSize = sizeof(sender);

        int bytes = recvfrom(
            sock,
            reinterpret_cast<char*>(&packet),
            sizeof(packet),
            0,
            (sockaddr*)&sender,
            &senderSize
        );

        if (bytes != sizeof(UDPPacket)) {
            std::cout << "bad packet size: got bytes=" << bytes << "\n";
            continue;
        }

        if (packet.imu.magic != 0xAA55) {
            std::cout << "bad magic=0x"
                      << std::hex << packet.imu.magic << std::dec
                      << "\n";
            continue;
        }

        uint64_t recv_us = now_us();

        double interval_ms = 0.0;
        if (last_recv_us != 0) {
            interval_ms = (recv_us - last_recv_us) / 1000.0;
        }
        last_recv_us = recv_us;

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