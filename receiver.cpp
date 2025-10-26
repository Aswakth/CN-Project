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
queue<BlastPacket> recv_queue;
bool done_receiving = false;

FileHeader negotiated_header;
int sock;

double packet_loss_percent = 0.0;
mt19937 rng(random_device{}());

void network_receiver_thread() {
    ofstream fout("recv_testfile.bin", ios::binary);
    if (!fout) { cerr << "Cannot create output file\n"; exit(1); }
    uint32_t blast_no = 0;
    uint64_t total_blasts_received = 0;
    uint64_t total_records_written = 0;
    uint64_t total_records_lost_sim = 0;
    uint64_t total_bytes_received = 0;
    chrono::steady_clock::time_point recv_start_time;
    chrono::steady_clock::time_point recv_end_time;

    while (!done_receiving) {
        char buf[65536];
        struct sockaddr_in sender_addr;
        socklen_t addrlen = sizeof(sender_addr);
        int n = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr *)&sender_addr, &addrlen);
        if (n <= 0) continue;

        // Prepare per-record loss distribution
        uniform_real_distribution<double> dist(0.0, 100.0);

        if (n == sizeof(FileHeader)) {
            memcpy(&negotiated_header, buf, sizeof(FileHeader));
            cout << "[Receiver] Received FILE_HDR, sending FILE_HDR_ACK\n";
            string ack = "FILE_HDR_ACK";
            sendto(sock, ack.c_str(), ack.size(), 0, (struct sockaddr *)&sender_addr, addrlen);
            continue;
        }

        // Detect if this is a REC_MISS (sender may send JSON/text messages later)
        // REC_MISS is ASCII JSON like [] or [[1,3],[5,5]] or {}. If we see a small ASCII message, log it.
        if (n <= 4096) {
            bool is_ascii = true;
            for (int i = 0; i < n; ++i) {
                unsigned char c = buf[i];
                if (c < 9 || (c > 13 && c < 32)) { is_ascii = false; break; }
            }
            if (is_ascii && (buf[0] == '[' || buf[0] == '{' || isalpha((unsigned char)buf[0]))) {
                string rec_recv(buf, n);
                cout << "[Receiver] Received REC_MISS from sender: " << rec_recv << "\n";
                // Try to parse array-of-ranges like [[1,3],[5,5]] or [] or {}
                vector<pair<uint32_t,uint32_t>> parsed;
                if (!rec_recv.empty() && rec_recv[0] == '[') {
                    // extract integers from the string
                    vector<long> nums;
                    long cur = 0;
                    bool in_num = false;
                    bool neg = false;
                    for (char ch : rec_recv) {
                        if (ch == '-') { neg = true; in_num = true; cur = 0; }
                        else if (isdigit((unsigned char)ch)) { in_num = true; cur = cur * 10 + (ch - '0'); }
                        else {
                            if (in_num) {
                                nums.push_back(neg ? -cur : cur);
                                cur = 0; in_num = false; neg = false;
                            }
                        }
                    }
                    if (in_num) nums.push_back(neg ? -cur : cur);
                    // pair up numbers
                    for (size_t i = 0; i + 1 < nums.size(); i += 2) {
                        long a = nums[i]; long b = nums[i+1];
                        if (a < 0) a = 0; if (b < 0) b = 0;
                        parsed.emplace_back((uint32_t)a, (uint32_t)b);
                    }
                }

                if (!parsed.empty()) {
                    cout << "[Receiver] Parsed REC_MISS ranges: ";
                    for (size_t i = 0; i < parsed.size(); ++i) {
                        if (i) cout << ", ";
                        cout << "(" << parsed[i].first << "," << parsed[i].second << ")";
                    }
                    cout << "\n";
                } else {
                    cout << "[Receiver] No missing records reported (REC_MISS empty)\n";
                }
                // For now we just log parsed REC_MISS; in future we could request retransmit based on these ranges.
                continue;
            }
        }

        if (n <= 11 && string(buf, n) == "DISCONNECT") {
            cout << "[Receiver] Sender disconnected\n";
            done_receiving = true;
            break;
        }

        // Deserialize BlastPacket
        BlastPacket pkt;
        memcpy(&pkt.num_segments, buf, sizeof(uint32_t));
        pkt.segments.resize(pkt.num_segments);
        memcpy(pkt.segments.data(), buf + sizeof(uint32_t), pkt.num_segments * sizeof(Segment));
        size_t data_len = n - sizeof(uint32_t) - pkt.num_segments * sizeof(Segment);
        pkt.data.resize(data_len);
        memcpy(pkt.data.data(), buf + sizeof(uint32_t) + pkt.num_segments * sizeof(Segment), data_len);

        // Write data
        uint32_t rec_size = negotiated_header.record_size;
        uint32_t offset = 0;
        vector<pair<uint32_t,uint32_t>> missing_ranges;
        for (auto &seg : pkt.segments) {
            for (uint32_t r = seg.start; r <= seg.end; ++r) {
                double p = dist(rng);
                if (p < packet_loss_percent) {
                    // mark this single record as missing
                    if (!missing_ranges.empty() && missing_ranges.back().second + 1 == r) {
                        missing_ranges.back().second = r;
                    } else {
                        missing_ranges.emplace_back(r, r);
                    }
                    // skip writing this record
                    total_records_lost_sim++;
                } else {
                    // write this record from pkt.data at current offset
                    fout.seekp((long)r * rec_size, ios::beg);
                    fout.write(pkt.data.data() + offset, rec_size);
                    total_records_written++;
                }
                offset += rec_size;
            }
        }

        // Build REC_MISS as JSON array of [start,end] ranges (or [] if none)
        string rec_miss = "[]";
        if (!missing_ranges.empty()) {
            stringstream ss;
            ss << "[";
            for (size_t i = 0; i < missing_ranges.size(); ++i) {
                if (i) ss << ",";
                ss << "[" << missing_ranges[i].first << "," << missing_ranges[i].second << "]";
            }
            ss << "]";
            rec_miss = ss.str();
        }
        
    sendto(sock, rec_miss.c_str(), rec_miss.size(), 0, (struct sockaddr *)&sender_addr, addrlen);
    cout << "[Receiver] Sent REC_MISS: " << rec_miss << "\n";
    // increment counts
    total_blasts_received++;
    total_bytes_received += (uint64_t)n;
    if (total_blasts_received == 1) recv_start_time = chrono::steady_clock::now();

        // Log received blast info
        if (!pkt.segments.empty()) {
            uint32_t start_rec = pkt.segments.front().start;
            uint32_t end_rec = pkt.segments.back().end;
            cout << "[Receiver] Received Blast " << (++blast_no) << " with records (" 
                 << start_rec << "-" << end_rec << ")\n";
        } else {
            cout << "[Receiver] Received Blast " << (++blast_no) << " with 0 segments\n";
        }
    }

    recv_end_time = chrono::steady_clock::now();
    fout.close();
    cout << "[Receiver] Generated file recv_testfile.bin\n";

    double secs = chrono::duration<double>(recv_end_time - recv_start_time).count();
    if (secs < 1e-6) secs = 1e-6;
    double throughput_bps = total_bytes_received / secs;
    cout << "[Receiver] Summary: blasts_received=" << total_blasts_received << ", bytes_received=" << total_bytes_received
         << ", records_written=" << total_records_written << ", records_lost_sim=" << total_records_lost_sim << "\n";
    cout << "[Receiver] Duration=" << secs << "s, Throughput=" << (throughput_bps) << " B/s (" << (throughput_bps*8/1e6) << " Mbps)\n";
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: ./receiver <packet_loss_percent>\n";
        return 1;
    }

    packet_loss_percent = stod(argv[1]);
    cout << "[Receiver] Listening on port 9000 with " << packet_loss_percent << "% packet loss...\n";

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in my_addr;
    memset(&my_addr, 0, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(9000);
    my_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sock, (struct sockaddr *)&my_addr, sizeof(my_addr)) < 0) {
        perror("bind"); exit(1);
    }

    thread t_net(network_receiver_thread);
    t_net.join();

    close(sock);
    return 0;
}
