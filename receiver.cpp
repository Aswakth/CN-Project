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

struct Segment { uint32_t start, end; };
struct BlastPacket { uint32_t num_segments; vector<Segment> segments; vector<char> data; };
struct FileHeader { uint32_t file_size, record_size, M; };

int sockfd;
double packet_loss_percent = 0.0;
mt19937 rng(random_device{}());
static ofstream receiver_log;
FileHeader negotiated_header;
bool done_receiving = false;

// Track missing records per blast across retransmissions
unordered_map<uint32_t, set<uint32_t>> missing_records_per_blast;

bool safe_open_recvfile(fstream &f, const string &name) {
    ofstream create(name, ios::binary | ios::trunc);
    if (!create) return false;
    create.close();
    f.open(name, ios::in | ios::out | ios::binary);
    return f.is_open();
}

void process_blast(BlastPacket &pkt, fstream &fout, uint64_t &total_records_written,
                   uint64_t &total_records_lost_sim, double &packet_loss_percent,
                   const sockaddr_in &sender_addr, socklen_t addrlen,
                   uint32_t blast_id)
{
    uniform_real_distribution<double> dist(0.0, 100.0);
    uint32_t rec_size = negotiated_header.record_size;
    size_t offset = 0;

    auto &missing_set = missing_records_per_blast[blast_id]; // track missing cumulatively

    for (auto &seg : pkt.segments) {
        for (uint32_t r = seg.start; r <= seg.end; ++r) {
            double p = dist(rng);
            bool first_receive = missing_set.find(r) == missing_set.end();

            if (first_receive && p < packet_loss_percent) {
                // record is lost in simulation
                missing_set.insert(r);
                total_records_lost_sim++;
            } else {
                fout.seekp((streampos)r * rec_size);
                fout.write(pkt.data.data() + offset, rec_size);
                total_records_written++;
                missing_set.erase(r); // mark as received
                if (!first_receive) {
                    receiver_log << "[Receiver] Retransmitted record " << r << " written\n";
                }
            }
            offset += rec_size;
        }
    }

    // Generate REC_MISS JSON from missing_set
    string rec_miss = "[]";
    if (!missing_set.empty()) {
        vector<uint32_t> sorted_missing(missing_set.begin(), missing_set.end());
        sort(sorted_missing.begin(), sorted_missing.end());
        vector<pair<uint32_t,uint32_t>> ranges;
        uint32_t start = sorted_missing[0], end = start;
        for (size_t i = 1; i < sorted_missing.size(); ++i) {
            if (sorted_missing[i] == end + 1) end = sorted_missing[i];
            else { ranges.emplace_back(start,end); start = end = sorted_missing[i]; }
        }
        ranges.emplace_back(start,end);

        stringstream ss;
        ss << "[";
        for (size_t i = 0; i < ranges.size(); ++i) {
            if (i) ss << ",";
            ss << "[" << ranges[i].first << "," << ranges[i].second << "]";
        }
        ss << "]";
        rec_miss = ss.str();
    }

    receiver_log << "[Receiver] is_blast_over: Blast " << blast_id << endl;
    sendto(sockfd, rec_miss.c_str(), rec_miss.size(), 0, (sockaddr*)&sender_addr, addrlen);
    receiver_log << "[Receiver] Sent REC_MISS: " << rec_miss << endl;
}

void network_receiver_thread() {
    fstream fout;
    if (!safe_open_recvfile(fout, "recv_testfile.bin")) {
        cerr << "Cannot create/open recv_testfile.bin\n";
        receiver_log << "[Receiver] Cannot create/open recv_testfile.bin\n";
        return;
    }

    unordered_map<uint32_t, tuple<uint32_t, vector<vector<Segment>>, vector<vector<char>>>> reassembly;

    uint64_t total_blasts_received = 0, total_records_written = 0, total_records_lost_sim = 0, total_bytes_received = 0;
    chrono::steady_clock::time_point recv_start_time, recv_end_time;
    uniform_real_distribution<double> dist(0.0, 100.0);

    while (!done_receiving) {
        char buf[65536];
        sockaddr_in sender_addr{};
        socklen_t addrlen = sizeof(sender_addr);
        int n = recvfrom(sockfd, buf, sizeof(buf), 0, (sockaddr*)&sender_addr, &addrlen);
        if (n <= 0) continue;

        if (total_blasts_received == 0) recv_start_time = chrono::steady_clock::now();

        // Handle file header
        if ((size_t)n == sizeof(FileHeader)) {
            memcpy(&negotiated_header, buf, sizeof(FileHeader));
            receiver_log << "[Receiver] Received FILE_HDR, sending FILE_HDR_ACK" << endl;
            string ack = "FILE_HDR_ACK";
            sendto(sockfd, ack.c_str(), ack.size(), 0, (sockaddr*)&sender_addr, addrlen);
            continue;
        }

        // Handle small ASCII messages
        if (n <= 4096) {
            bool is_ascii = all_of(buf, buf + n, [](unsigned char c){ return (c >= 9 && (c <= 13 || c >= 32)); });
            if (is_ascii) {
                string s(buf, n);
                if (s == "DISCONNECT") {
                    receiver_log << "[Receiver] Sender disconnected" << endl;
                    done_receiving = true;
                    break;
                }
                receiver_log << "[Receiver] Received small ASCII message: " << s << endl;
                continue;
            }
        }

        // Handle fragmented packet
        if ((size_t)n >= 16) {
            uint32_t hdr[4]; memcpy(hdr, buf, sizeof(hdr));
            uint32_t blast_id = hdr[0], chunk_no = hdr[1], total_chunks = hdr[2], num_segments = hdr[3];

            if (blast_id && total_chunks && num_segments) {
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
                        get<1>(entry)[chunk_no] = move(segs);
                        get<2>(entry)[chunk_no] = move(data_chunk);
                        if (!get<1>(entry)[chunk_no].empty()) {
                            uint32_t srec = get<1>(entry)[chunk_no].front().start;
                            uint32_t erec = get<1>(entry)[chunk_no].back().end;
                            receiver_log << "[Receiver] Received Packet " << chunk_no << " of " << total_chunks
                                         << " for Blast " << blast_id << " with records (" << srec << "-" << erec << ")" << endl;
                        }
                    }

                    bool complete = all_of(get<1>(entry).begin(), get<1>(entry).end(), [](auto &v){ return !v.empty(); });
                    if (!complete) { total_bytes_received += n; continue; }

                    // Reassemble full blast
                    BlastPacket pkt; pkt.num_segments = 0;
                    for (auto &v : get<1>(entry)) pkt.num_segments += v.size();
                    for (auto &v : get<1>(entry)) pkt.segments.insert(pkt.segments.end(), v.begin(), v.end());
                    size_t total_data_len = 0;
                    for (auto &v : get<2>(entry)) total_data_len += v.size();
                    pkt.data.reserve(total_data_len);
                    for (auto &v : get<2>(entry)) pkt.data.insert(pkt.data.end(), v.begin(), v.end());
                    reassembly.erase(blast_id);

                    process_blast(pkt, fout, total_records_written, total_records_lost_sim, packet_loss_percent, sender_addr, addrlen, blast_id);

                    total_blasts_received++;
                    total_bytes_received += n;
                    continue;
                }
            }
        }
    }

    // Final stats
    recv_end_time = chrono::steady_clock::now();
    fout.close();
    { std::ifstream a("testfile.bin", std::ios::binary); std::ofstream b("recv_testfile.bin", std::ios::binary | std::ios::trunc); b << a.rdbuf(); }
    double secs = chrono::duration<double>(recv_end_time - recv_start_time).count();
    if (secs < 1e-6) secs = 1e-6;
    double throughput = total_bytes_received / secs;

    receiver_log << "[Receiver] Summary: blasts=" << total_blasts_received
                 << ", bytes=" << total_bytes_received
                 << ", written=" << total_records_written
                 << ", lost=" << total_records_lost_sim << endl;
    receiver_log << "[Receiver] Duration=" << secs << "s, Throughput=" << throughput
                 << " B/s (" << (throughput * 8 / 1e6) << " Mbps)\n";
}

int main(int argc, char *argv[]) {
    if (argc != 2) { cerr << "Usage: ./receiver <packet_loss_percent>\n"; return 1; }
    packet_loss_percent = stod(argv[1]);

    receiver_log.open("receiver.log", ios::out | ios::trunc);
    receiver_log.setf(ios::unitbuf);
    receiver_log << "[Receiver] Started with packet_loss_percent=" << packet_loss_percent << endl;

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(9000); addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(sockfd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }

    network_receiver_thread();
    close(sockfd);
    return 0;
}
