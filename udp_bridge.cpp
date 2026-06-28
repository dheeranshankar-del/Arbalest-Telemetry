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

static const char* SERIAL_PORT = "/dev/cu.usbmodem11101"; // this says read from Arduino Serial Port (IMU Data)
static const char* WINDOWS_IP = "192.168.10.2";// my windows machine's IP 
static const int UDP_PORT = 5005; // This is the UDP port to use

#pragma pack(push, 1) // this is all the data coming in from the arduino
struct IMUData {
    uint16_t magic;
    uint32_t t;
    float roll;
    float pitch; // orientation
    float yaw;
    float ax; //acceleration due to gravity 9.81m/s
    float ay;
    float az;
};

struct UDPPacket {
    IMUData imu;
    uint64_t mac_send_us; // gets arduino data and puts a Mac time stamp
};
#pragma pack(pop)


// This gets the Mac time in microseconds
uint64_t now_us() {
    auto now = std::chrono::steady_clock::now();
    return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch() 
    ).count();
}


// This is the File Descriptor for the open Serial Port
int read_exact(int fd, uint8_t* buf, int len) {
    int got = 0;

    while (got < len) {
        int n = read(fd, buf + got, len - got); // buf points to the start of buffer got is how many bytes we've received

        if (n > 0) {
            got += n;
        } else {
            usleep(1000);
        }
    }

    return got;
}


int main() {
    int serial_fd = open(SERIAL_PORT, O_RDWR | O_NOCTTY | O_NONBLOCK); //opens the serial port for R & W
    if (serial_fd < 0) { //checks whether open failed
        perror("open serial");
        return 1;
    }

    // this stores serial port settings like baud rate,
    termios tty{};
    if (tcgetattr(serial_fd, &tty) != 0) {
        perror("tcgetattr");
        return 1;
    }


    //configures serial port
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

    
// This part sets up the UDP netowrking
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }
//sets destination port
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(UDP_PORT);

    if (inet_pton(AF_INET, WINDOWS_IP, &dest.sin_addr) != 1) {
        std::cerr << "Bad WINDOWS_IP: " << WINDOWS_IP << "\n";
        return 1;
    }

    
// creates empty packet and prints startup info
    UDPPacket packet{};

    std::cout << "Bridge running\n";
    std::cout << "Sending to " << WINDOWS_IP << ":" << UDP_PORT << "\n";
    std::cout << "IMU size = " << sizeof(IMUData) << " bytes\n";
    std::cout << "UDP packet size = " << sizeof(UDPPacket) << " bytes\n";

    // infinite loop keeps running unless you stop the program
    while (true) {
        uint8_t b = 0;

        while (true) {
            int n = read(serial_fd, &b, 1); //reads serial bytes 

            if (n > 0) { // prints every byte for debugging
                std::cout << "serial byte=0x"
                          << std::hex << (int)b << std::dec
                          << "\n"; // 
            }

            if (n > 0 && b == 0x55) { // looks for the first header of 0x55 byte
                uint8_t b2 = 0;
                read_exact(serial_fd, &b2, 1);

                std::cout << "second byte=0x"
                          << std::hex << (int)b2 << std::dec
                          << "\n";

                if (b2 == 0xAA) { //checks second header byte 0xAA
                    std::cout << "found header 55 AA\n";
                    break;
                }
            } else {
                usleep(1000); // sleeps if no useful byte
            }
        }

        packet.imu.magic = 0xAA55; // fills first 2 bytes if packet struct

        uint8_t* rest = reinterpret_cast<uint8_t*>(&packet.imu) + 2; //read rest of IMU packet
        read_exact(serial_fd, rest, sizeof(IMUData) - 2);

        packet.mac_send_us = now_us(); // Mac Timestamp

        errno = 0;

        ssize_t sent = sendto(// send packet over UDP
            sock,
            reinterpret_cast<const char*>(&packet),
            sizeof(packet),
            0,
            reinterpret_cast<sockaddr*>(&dest),
            sizeof(dest)
        );

        std::cout << "sent bytes=" << sent //prints send results
                  << " expected=" << sizeof(packet)
                  << " errno=" << errno;

        if (sent < 0) {
            std::cout << " error=" << strerror(errno); // print error if sending failed
        }

        std::cout << "\n";
    }

    close(sock);
    close(serial_fd);
    return 0;
}
