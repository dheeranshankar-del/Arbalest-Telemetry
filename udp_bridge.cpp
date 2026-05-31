#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>
#include <IOKit/serial/ioss.h>

#include <chrono>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <iostream>

static const char* SERIAL_PORT = "/dev/cu.usbmodem11101";
static const char* WINDOWS_IP = "192.168.10.2";
static const int UDP_PORT = 5005;

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

int read_exact(int fd, uint8_t* buf, int len) {
    int got = 0;

    while (got < len) {
        int n = read(fd, buf + got, len - got);

        if (n > 0) {
            got += n;
        } else {
            usleep(1000);
        }
    }

    return got;
}

int main() {
    int serial_fd = open(SERIAL_PORT, O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (serial_fd < 0) {
        perror("open serial");
        return 1;
    }

    termios tty{};
    if (tcgetattr(serial_fd, &tty) != 0) {
        perror("tcgetattr");
        return 1;
    }

    cfmakeraw(&tty);
    tty.c_cflag |= CLOCAL | CREAD;

    if (tcsetattr(serial_fd, TCSANOW, &tty) != 0) {
        perror("tcsetattr");
        return 1;
    }

    speed_t speed = 500000;
    if (ioctl(serial_fd, IOSSIOSPEED, &speed) != 0) {
        perror("IOSSIOSPEED");
        return 1;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(UDP_PORT);

    if (inet_pton(AF_INET, WINDOWS_IP, &dest.sin_addr) != 1) {
        std::cerr << "Bad WINDOWS_IP: " << WINDOWS_IP << "\n";
        return 1;
    }

    UDPPacket packet{};

    std::cout << "Bridge running\n";
    std::cout << "Sending to " << WINDOWS_IP << ":" << UDP_PORT << "\n";
    std::cout << "IMU size = " << sizeof(IMUData) << " bytes\n";
    std::cout << "UDP packet size = " << sizeof(UDPPacket) << " bytes\n";

    while (true) {
        uint8_t b = 0;

        while (true) {
            int n = read(serial_fd, &b, 1);

            if (n > 0) {
                std::cout << "serial byte=0x"
                          << std::hex << (int)b << std::dec
                          << "\n";
            }

            if (n > 0 && b == 0x55) {
                uint8_t b2 = 0;
                read_exact(serial_fd, &b2, 1);

                std::cout << "second byte=0x"
                          << std::hex << (int)b2 << std::dec
                          << "\n";

                if (b2 == 0xAA) {
                    std::cout << "found header 55 AA\n";
                    break;
                }
            } else {
                usleep(1000);
            }
        }

        packet.imu.magic = 0xAA55;

        uint8_t* rest = reinterpret_cast<uint8_t*>(&packet.imu) + 2;
        read_exact(serial_fd, rest, sizeof(IMUData) - 2);

        packet.mac_send_us = now_us();

        errno = 0;

        ssize_t sent = sendto(
            sock,
            reinterpret_cast<const char*>(&packet),
            sizeof(packet),
            0,
            reinterpret_cast<sockaddr*>(&dest),
            sizeof(dest)
        );

        std::cout << "sent bytes=" << sent
                  << " expected=" << sizeof(packet)
                  << " errno=" << errno;

        if (sent < 0) {
            std::cout << " error=" << strerror(errno);
        }

        std::cout << "\n";
    }

    close(sock);
    close(serial_fd);
    return 0;
}