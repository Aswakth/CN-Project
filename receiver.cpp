// receiver.cpp
#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <random>

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

int sockfd;
double packet_loss_percent = 0.0;
mt19937 rng(random_device{}());

// receiver log
static std::ofstream receiver_log;
FileHeader negotiated_header;
bool done_receiving = false;

bool safe_open_recvfile(fstream &f, const string &name) {
    // Create/truncate the file using ofstream, then reopen as fstream (in|out) for random-access writes.
    ofstream create(name, ios::binary | ios::trunc);
    if (!create) return false;
    create.close();
    f.open(name, ios::in | ios::out | ios::binary);
    if (!f) return false;
    return true;
}

void network_receiver_thread() {
    fstream fout;
    if (!safe_open_recvfile(fout, "recv_testfile.bin")) {
        cerr << "Cannot create/open recv_testfile.bin\n";
        receiver_log << "[Receiver] Cannot create/open recv_testfile.bin\n";
        return;
    }

    uniform_real_distribution<double> dist(0.0, 100.0);

    unordered_map<uint32_t, tuple<uint32_t, vector<vector<Segment>>, vector<vector<char>>>> reassembly;
    // tuple: total_chunks, segments_per_chunk, data_per_chunk

    uint64_t total_blasts_received = 0;
    uint64_t total_records_written = 0;
    uint64_t total_records_lost_sim = 0;
    uint64_t total_bytes_received = 0;
    chrono::steady_clock::time_point recv_start_time, recv_end_time;

    while (!done_receiving) {
        char buf[65536];
        struct sockaddr_in sender_addr;
        socklen_t addrlen = sizeof(sender_addr);
        int n = recvfrom(sockfd, buf, sizeof(buf), 0, (struct sockaddr *)&sender_addr, &addrlen);
        if (n <= 0) continue;

        if (total_blasts_received == 0) recv_start_time = chrono::steady_clock::now();

        // If datagram matches FileHeader size -> treat as header
        if ((size_t)n == sizeof(FileHeader)) {
            memcpy(&negotiated_header, buf, sizeof(FileHeader));
            receiver_log << "[Receiver] Received FILE_HDR, sending FILE_HDR_ACK" << endl;
            string ack = "FILE_HDR_ACK";
            sendto(sockfd, ack.c_str(), ack.size(), 0, (struct sockaddr *)&sender_addr, addrlen);
            continue;
        }

        // small ASCII messages: REC_MISS or DISCONNECT
        if (n <= 4096) {
            bool is_ascii = true;
            for (int i = 0; i < n; ++i) {
                unsigned char c = buf[i];
                if (c < 9 || (c > 13 && c < 32)) { is_ascii = false; break; }
            }
            if (is_ascii) {
                string s(buf, n);
                if (s == "DISCONNECT") {
                    receiver_log << "[Receiver] Sender disconnected" << endl;
                    done_receiving = true;
                    break;
                }
                // treat as REC_MISS-like / text log
                receiver_log << "[Receiver] Received small ASCII message: " << s << endl;
                // don't try to parse further here (sender expects REC_MISS from us)
                continue;
            }
        }

        // Check for fragmented-chunk header (>= 16 bytes)
        if ((size_t)n >= 16) {
            uint32_t hdr[4];
            memcpy(hdr, buf, sizeof(hdr));
            uint32_t blast_id = hdr[0];
            uint32_t chunk_no = hdr[1];
            uint32_t total_chunks = hdr[2];
            uint32_t num_segments = hdr[3];

            if (blast_id != 0 && total_chunks >= 1 && num_segments >= 1) {
                size_t segs_offset = 16;
                if ((size_t)n < segs_offset + num_segments * sizeof(Segment)) {
                    receiver_log << "[Receiver] malformed fragment packet: too small for segments\n";
                } else {
                    vector<Segment> segs(num_segments);
                    memcpy(segs.data(), buf + segs_offset, num_segments * sizeof(Segment));
                    size_t data_offset = segs_offset + num_segments * sizeof(Segment);
                    size_t data_len = (size_t)n - data_offset;
                    vector<char> data_chunk(data_len);
                    memcpy(data_chunk.data(), buf + data_offset, data_len);

                    auto &entry = reassembly[blast_id];
                    if (get<0>(entry) == 0) {
                        get<0>(entry) = total_chunks;
                        get<1>(entry).resize(total_chunks);
                        get<2>(entry).resize(total_chunks);
                    }
                    if (chunk_no < get<0>(entry)) {
                        get<1>(entry)[chunk_no] = std::move(segs);
                        get<2>(entry)[chunk_no] = std::move(data_chunk);

                        // log
                        if (!get<1>(entry)[chunk_no].empty()) {
                            uint32_t srec = get<1>(entry)[chunk_no].front().start;
                            uint32_t erec = get<1>(entry)[chunk_no].back().end;
                            receiver_log << "[Receiver] Received Packet " << chunk_no << " of " << total_chunks
                                         << " for Blast " << blast_id << " with records (" << srec << "-" << erec << ")" << endl;
                        }
                    }

                    // check completeness
                    bool complete = true;
                    for (uint32_t i = 0; i < get<0>(entry); ++i) {
                        if (get<1>(entry)[i].empty()) { complete = false; break; }
                    }

                    if (!complete) {
                        total_bytes_received += (uint64_t)n;
                        continue;
                    }

                    // reconstruct BlastPacket
                    BlastPacket pkt;
                    pkt.num_segments = 0;
                    for (uint32_t i = 0; i < get<0>(entry); ++i) pkt.num_segments += (uint32_t)get<1>(entry)[i].size();
                    pkt.segments.reserve(pkt.num_segments);
                    for (uint32_t i = 0; i < get<0>(entry); ++i) {
                        for (auto &s : get<1>(entry)[i]) pkt.segments.push_back(s);
                    }
                    // concatenate data chunks in chunk order
                    size_t total_data_len = 0;
                    for (uint32_t i = 0; i < get<0>(entry); ++i) total_data_len += get<2>(entry)[i].size();
                    pkt.data.resize(total_data_len);
                    size_t pos = 0;
                    for (uint32_t i = 0; i < get<0>(entry); ++i) {
                        memcpy(pkt.data.data() + pos, get<2>(entry)[i].data(), get<2>(entry)[i].size());
                        pos += get<2>(entry)[i].size();
                    }

                    // erase reassembly entry
                    reassembly.erase(blast_id);

                    // write pkt to file w/ simulated loss
                    uint32_t rec_size = negotiated_header.record_size;
                    size_t offset = 0;
                    vector<pair<uint32_t,uint32_t>> missing_ranges;
                    for (auto &seg : pkt.segments) {
                        for (uint32_t r = seg.start; r <= seg.end; ++r) {
                            double p = dist(rng);
                            if (p < packet_loss_percent) {
                                if (!missing_ranges.empty() && missing_ranges.back().second + 1 == r) {
                                    missing_ranges.back().second = r;
                                } else {
                                    missing_ranges.emplace_back(r, r);
                                }
                                total_records_lost_sim++;
                            } else {
                                fout.seekp((streampos)r * rec_size, ios::beg);
                                fout.write(pkt.data.data() + offset, rec_size);
                                total_records_written++;
                            }
                            offset += rec_size;
                        }
                    }

                    // send REC_MISS JSON (or [] if none)
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
                    sendto(sockfd, rec_miss.c_str(), rec_miss.size(), 0, (struct sockaddr *)&sender_addr, addrlen);
                    receiver_log << "[Receiver] Sent REC_MISS: " << rec_miss << endl;
                    receiver_log << "[Receiver] is_blast_over: Blast " << blast_id << endl;

                    total_blasts_received++;
                    total_bytes_received += (uint64_t)n;
                    continue;
                }
            }
        }

        // Legacy single-datagram format fallback
        BlastPacket pkt;
        if ((size_t)n < sizeof(uint32_t)) {
            receiver_log << "[Receiver] malformed legacy packet: too small\n";
            continue;
        }
        memcpy(&pkt.num_segments, buf, sizeof(uint32_t));
        pkt.segments.resize(pkt.num_segments);
        size_t need = sizeof(uint32_t) + pkt.num_segments * sizeof(Segment);
        if ((size_t)n < need) {
            receiver_log << "[Receiver] malformed legacy packet: too small for segments\n";
            continue;
        }
        memcpy(pkt.segments.data(), buf + sizeof(uint32_t), pkt.num_segments * sizeof(Segment));
        size_t data_len = (size_t)n - need;
        pkt.data.resize(data_len);
        memcpy(pkt.data.data(), buf + need, data_len);

        // process legacy pkt
        uint32_t rec_size = negotiated_header.record_size;
        size_t offset = 0;
        vector<pair<uint32_t,uint32_t>> missing_ranges;
        for (auto &seg : pkt.segments) {
            for (uint32_t r = seg.start; r <= seg.end; ++r) {
                double p = dist(rng);
                if (p < packet_loss_percent) {
                    if (!missing_ranges.empty() && missing_ranges.back().second + 1 == r) {
                        missing_ranges.back().second = r;
                    } else {
                        missing_ranges.emplace_back(r, r);
                    }
                    total_records_lost_sim++;
                } else {
                    fout.seekp((streampos)r * rec_size, ios::beg);
                    fout.write(pkt.data.data() + offset, rec_size);
                    total_records_written++;
                }
                offset += rec_size;
            }
        }

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
        sendto(sockfd, rec_miss.c_str(), rec_miss.size(), 0, (struct sockaddr *)&sender_addr, addrlen);
        receiver_log << "[Receiver] Sent REC_MISS: " << rec_miss << endl;

        total_blasts_received++;
        total_bytes_received += (uint64_t)n;

        // Log blast boundaries (legacy)
        if (!pkt.segments.empty()) {
            uint32_t start_rec = pkt.segments.front().start;
            uint32_t end_rec = pkt.segments.back().end;
            receiver_log << "[Receiver] Received Blast " << total_blasts_received << " with records ("
                << start_rec << "-" << end_rec << ")" << endl;
        } else {
            receiver_log << "[Receiver] Received Blast " << total_blasts_received << " with 0 segments" << endl;
        }
        receiver_log << "[Receiver] is_blast_over: Blast " << total_blasts_received << endl;
    }

    // Dump incomplete reassembly info
    if (!reassembly.empty()) {
        receiver_log << "[Receiver] Incomplete reassembly entries at shutdown:\n";
        for (auto &kv : reassembly) {
            uint32_t bid = kv.first;
            receiver_log << "  Blast " << bid << " waiting for " << get<0>(kv.second) << " chunks\n";
        }
    }

    recv_end_time = chrono::steady_clock::now();
    fout.flush();
    fout.close();

    receiver_log << "[Receiver] Generated file recv_testfile.bin\n";

    double secs = chrono::duration<double>(recv_end_time - recv_start_time).count();
    if (secs < 1e-6) secs = 1e-6;
    double throughput_bps = (double)total_bytes_received / secs;

    receiver_log << "[Receiver] Summary: blasts_received=" << total_blasts_received << ", bytes_received=" << total_bytes_received
                 << ", records_written=" << total_records_written << ", records_lost_sim=" << total_records_lost_sim << endl;
    receiver_log << "[Receiver] Duration=" << secs << "s, Throughput=" << (throughput_bps)
                 << " B/s (" << (throughput_bps*8/1e6) << " Mbps)" << endl;

    receiver_log.flush();
    receiver_log.close();

    cout << "[Receiver] Summary: blasts_received=" << total_blasts_received << ", bytes_received=" << total_bytes_received
         << ", records_written=" << total_records_written << ", records_lost_sim=" << total_records_lost_sim << endl;
    cout << "[Receiver] Duration=" << secs << "s, Throughput=" << (throughput_bps)
         << " B/s (" << (throughput_bps*8/1e6) << " Mbps)" << endl;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: ./receiver <packet_loss_percent>\n";
        return 1;
    }

    packet_loss_percent = stod(argv[1]);

    receiver_log.open("receiver.log", ios::out | ios::trunc);
    if (!receiver_log.is_open()) {
        cerr << "Unable to open receiver.log for writing\n";
    } else {
        receiver_log << std::unitbuf;
        receiver_log << "[Receiver] Started with packet_loss_percent=" << packet_loss_percent << endl;
    }

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) { perror("socket"); return 1; }

    struct sockaddr_in my_addr;
    memset(&my_addr, 0, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(9000);
    my_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(my_addr)) < 0) {
        perror("bind");
        close(sockfd);
        return 1;
    }

    // run receiver loop in current thread
    network_receiver_thread();

    close(sockfd);
    return 0;
}
