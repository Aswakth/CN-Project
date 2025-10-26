// sender.cpp
#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>

using namespace std;

struct Segment {
    uint32_t start;
    uint32_t end;
};

struct BlastPacket {
    uint32_t num_segments;
    vector<Segment> segments;
    vector<char> data;
};

struct FileHeader {
    uint32_t file_size;
    uint32_t record_size;
    uint32_t M;
};

mutex mtx;
condition_variable cv;
queue<BlastPacket> blast_queue;
bool done_reading = false;

FileHeader negotiated_header;

int sockfd;
struct sockaddr_in receiver_addr;

// Sender log file
static std::ofstream sender_log;

// Performance counters
uint64_t total_logical_blasts_sent = 0;
uint64_t total_packets_sent = 0;
uint64_t total_bytes_sent = 0;
uint64_t total_rec_miss_msgs = 0;
uint64_t total_missing_records_reported = 0;
chrono::steady_clock::time_point send_start_time;
chrono::steady_clock::time_point send_end_time;

void disk_read_thread(const string &filename) {
    ifstream fin(filename, ios::binary);
    if (!fin) {
        sender_log << "[DiskRead] Cannot open file: " << filename << endl;
        done_reading = true;
        cv.notify_one();
        return;
    }

    uint32_t record_size = negotiated_header.record_size;
    uint32_t M = negotiated_header.M;
    uint32_t total_records = (negotiated_header.file_size + record_size - 1) / record_size;
    uint32_t record_no = 0;

    while (record_no < total_records) {
        BlastPacket pkt;
        pkt.num_segments = 0;
        pkt.data.clear();
        uint32_t records_in_this_blast = min(M, total_records - record_no);

        for (uint32_t i = 0; i < records_in_this_blast; ++i) {
            vector<char> rec(record_size);
            fin.read(rec.data(), record_size);
            if ((size_t)fin.gcount() < record_size) {
                size_t got = (size_t)fin.gcount();
                fill(rec.begin() + got, rec.end(), 0);
            }
            pkt.segments.push_back({record_no, record_no});
            pkt.num_segments++;
            pkt.data.insert(pkt.data.end(), rec.begin(), rec.end());
            record_no++;
        }

        {
            unique_lock<mutex> lock(mtx);
            blast_queue.push(std::move(pkt));
        }
        cv.notify_one();
    }

    done_reading = true;
    cv.notify_one();
}

void send_packet(const BlastPacket &pkt, uint32_t logical_id) {
    const uint32_t RECORDS_PER_PACKET = 16;
    uint32_t total_packets = (pkt.num_segments + RECORDS_PER_PACKET - 1) / RECORDS_PER_PACKET;
    size_t rec_size = negotiated_header.record_size;

    for (uint32_t packet = 0; packet < total_packets; ++packet) {
        uint32_t start_idx = packet * RECORDS_PER_PACKET;
        uint32_t end_idx = min<uint32_t>((uint32_t)pkt.num_segments - 1, start_idx + RECORDS_PER_PACKET - 1);
        uint32_t num_segments_in_packet = end_idx - start_idx + 1;

        size_t header_size = sizeof(uint32_t) * 4;
        size_t packet_size = header_size + num_segments_in_packet * sizeof(Segment) + num_segments_in_packet * rec_size;
        vector<char> send_buf(packet_size);

        uint32_t header_buf[4] = {logical_id, packet, total_packets, num_segments_in_packet};
        memcpy(send_buf.data(), header_buf, header_size);
        memcpy(send_buf.data() + header_size, pkt.segments.data() + start_idx, num_segments_in_packet * sizeof(Segment));
        size_t data_offset = (size_t)start_idx * rec_size;
        memcpy(send_buf.data() + header_size + num_segments_in_packet * sizeof(Segment),
               pkt.data.data() + data_offset, num_segments_in_packet * rec_size);

        ssize_t s = sendto(sockfd, send_buf.data(), packet_size, 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));
        if (s == -1) {
            sender_log << "[Sender] sendto error: " << strerror(errno) << endl;
        } else {
            total_packets_sent++;
            total_bytes_sent += (size_t)s;
            sender_log << "[Sender] Sent packet " << packet << " of " << total_packets
                       << " for blast " << logical_id << " (size=" << s << ")" << endl;
        }
    }
}

void network_sender_thread() {
    uint32_t blast_no = 0;
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    while (true) {
        BlastPacket pkt;
        {
            unique_lock<mutex> lock(mtx);
            cv.wait(lock, [] { return !blast_queue.empty() || done_reading; });

            if (blast_queue.empty() && done_reading) break;
            pkt = std::move(blast_queue.front());
            blast_queue.pop();
        }

        uint32_t logical_id = ++blast_no;

        if (total_logical_blasts_sent == 0) send_start_time = chrono::steady_clock::now();

        // Send original blast
        send_packet(pkt, logical_id);

        total_logical_blasts_sent++;
        sender_log << "[Sender] Blast " << logical_id << " sent with records (" 
                   << pkt.segments.front().start << "-" << pkt.segments.back().end << ")" << endl;
        sender_log << "[Sender] is_blast_over: Blast " << logical_id << endl;

        // Wait for REC_MISS
        char buf[8192];
        socklen_t addrlen = sizeof(receiver_addr);
        ssize_t rn = recvfrom(sockfd, buf, sizeof(buf), 0, (struct sockaddr *)&receiver_addr, &addrlen);

        if (rn <= 0) {
            sender_log << "[Sender] No REC_MISS received (timeout or error) for blast " << logical_id << endl;
            continue;
        }

        string rec_miss(buf, (size_t)rn);
        sender_log << "[Sender] REC_MISS for blast " << logical_id << ": " << rec_miss << endl;
        total_rec_miss_msgs++;

        // Parse REC_MISS ranges
        vector<pair<uint32_t,uint32_t>> missing_ranges;
        long cur = 0; bool in_num = false; bool neg = false;
        vector<long> nums;
        for (char ch : rec_miss) {
            if (ch == '-') { neg = true; in_num = true; cur = 0; }
            else if (isdigit((unsigned char)ch)) { in_num = true; cur = cur*10 + (ch - '0'); }
            else { if (in_num) { nums.push_back(neg?-cur:cur); cur=0; in_num=false; neg=false; } }
        }
        if (in_num) nums.push_back(neg?-cur:cur);
        for (size_t i = 0; i + 1 < nums.size(); i+=2) missing_ranges.emplace_back(nums[i], nums[i+1]);

        // Count total missing records
        for (auto &p : missing_ranges) total_missing_records_reported += (p.second - p.first + 1);

        // --- RETRANSMIT missing records ---
        if (!missing_ranges.empty()) {
            sender_log << "[Sender] Retransmitting missing records for blast " << logical_id << endl;
            for (auto &range : missing_ranges) {
                BlastPacket retrans_pkt;
                retrans_pkt.num_segments = 0;
                retrans_pkt.data.clear();

                uint32_t rec_size = negotiated_header.record_size;
                ifstream fin("inputfile.bin", ios::binary); // make sure the filename is same as input
                fin.seekg((streampos)range.first * rec_size);

                for (uint32_t r = range.first; r <= range.second; ++r) {
                    vector<char> rec(rec_size);
                    fin.read(rec.data(), rec_size);
                    retrans_pkt.segments.push_back({r,r});
                    retrans_pkt.num_segments++;
                    retrans_pkt.data.insert(retrans_pkt.data.end(), rec.begin(), rec.end());
                }
                send_packet(retrans_pkt, logical_id); // reuse same logical_id for retransmit
                sender_log << "[Sender] Retransmitted records (" << range.first << "-" << range.second 
                           << ") for blast " << logical_id << endl;
            }
        }
    }

    // Send DISCONNECT
    string disc = "DISCONNECT";
    sendto(sockfd, disc.c_str(), disc.size(), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));
    send_end_time = chrono::steady_clock::now();
    sender_log << "[Sender] DISCONNECTED" << endl;

    double secs = chrono::duration<double>(send_end_time - send_start_time).count();
    if (secs < 1e-6) secs = 1e-6;
    double throughput_bps = (double)total_bytes_sent / secs;

    sender_log << "[Sender] Summary: logical_blasts_sent=" << total_logical_blasts_sent
               << ", packets_sent=" << total_packets_sent << ", bytes_sent=" << total_bytes_sent
               << ", rec_miss_msgs=" << total_rec_miss_msgs
               << ", missing_records_reported=" << total_missing_records_reported << endl;
    sender_log << "[Sender] Duration=" << secs << "s, Throughput=" << (throughput_bps)
               << " B/s (" << (throughput_bps*8/1e6) << " Mbps)" << endl;
    sender_log.flush();
    sender_log.close();

    cout << "[Sender] Duration=" << secs << "s, Throughput=" << (throughput_bps)
         << " B/s (" << (throughput_bps*8/1e6) << " Mbps)" << endl;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cerr << "Usage: ./sender <file> <receiver_ip>\n";
        return 1;
    }

    string filename = argv[1];
    string ip = argv[2];

    sender_log.open("sender.log", ios::out | ios::trunc);
    if (!sender_log.is_open()) {
        cerr << "Unable to open sender.log for writing\n";
    } else sender_log << std::unitbuf << "[Sender] Log started\n";

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) { perror("socket"); return 1; }

    memset(&receiver_addr, 0, sizeof(receiver_addr));
    receiver_addr.sin_family = AF_INET;
    receiver_addr.sin_port = htons(9000);
    if (inet_pton(AF_INET, ip.c_str(), &receiver_addr.sin_addr) != 1) {
        cerr << "Invalid receiver IP\n";
        close(sockfd);
        return 1;
    }

    // Prepare header
    ifstream fin(filename, ios::binary | ios::ate);
    if (!fin) { cerr << "Cannot open input file: " << filename << "\n"; close(sockfd); return 1; }
    streampos fsize = fin.tellg(); fin.close();
    negotiated_header.file_size = (uint32_t)max((streampos)0, fsize);
    negotiated_header.record_size = 512;
    negotiated_header.M = 500; // forced
    sender_log << "[Sender] Forcing records-per-blast M=" << negotiated_header.M
               << " (record_size=" << negotiated_header.record_size << ", file_size=" << negotiated_header.file_size << ")\n";

    ssize_t s = sendto(sockfd, &negotiated_header, sizeof(negotiated_header), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));
    if (s <= 0) sender_log << "[Sender] Failed to send FILE_HDR\n";
    else sender_log << "[Sender] Sent FILE_HDR\n";

    // Wait for ACK
    struct timeval tv;
    tv.tv_sec = 2; tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
    char ack[64]; socklen_t addrlen = sizeof(receiver_addr);
    ssize_t rn = recvfrom(sockfd, ack, sizeof(ack), 0, (struct sockaddr *)&receiver_addr, &addrlen);
    if (rn <= 0) sender_log << "[Sender] No FILE_HDR_ACK received (continuing anyway)\n";
    else sender_log << "[Sender] Received FILE_HDR_ACK\n";

    tv.tv_sec = 0; tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    thread t_disk(disk_read_thread, filename);
    thread t_net(network_sender_thread);
    t_disk.join();
    t_net.join();

    close(sockfd);
    return 0;
}
