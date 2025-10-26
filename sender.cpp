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
    vector<char> data; // dynamic data to avoid stack smashing
};

struct FileHeader {
    uint32_t file_size;
    uint32_t record_size;
    uint32_t M; // records per blast
};

mutex mtx;
condition_variable cv;
queue<BlastPacket> blast_queue;
bool done_reading = false;

FileHeader negotiated_header;

int sock;
struct sockaddr_in receiver_addr;

// Performance counters
uint64_t total_blasts_sent = 0;
uint64_t total_bytes_sent = 0;
uint64_t total_rec_miss_msgs = 0;
uint64_t total_missing_records_reported = 0;
std::chrono::steady_clock::time_point send_start_time;
std::chrono::steady_clock::time_point send_end_time;

void disk_read_thread(const string &filename) {
    ifstream fin(filename, ios::binary);
    if (!fin) {
        cerr << "Cannot open file\n";
        exit(1);
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

        for (uint32_t i = 0; i < records_in_this_blast; i++) {
            vector<char> rec(record_size);
            fin.read(rec.data(), record_size);
            pkt.segments.push_back({record_no, record_no});
            pkt.num_segments++;
            pkt.data.insert(pkt.data.end(), rec.begin(), rec.end());
            record_no++;
        }

        {
            unique_lock<mutex> lock(mtx);
            blast_queue.push(pkt);
        }
        cv.notify_one();
    }

    done_reading = true;
    cv.notify_one();
}

void network_sender_thread() {
    uint32_t blast_no = 0;
    while (true) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [] { return !blast_queue.empty() || done_reading; });

        if (blast_queue.empty() && done_reading) break;

        BlastPacket pkt = blast_queue.front();
        blast_queue.pop();
        lock.unlock();

        // Serialize packet
        size_t pkt_size = sizeof(uint32_t) + pkt.num_segments * sizeof(Segment) + pkt.data.size();
        vector<char> send_buf(pkt_size);
        memcpy(send_buf.data(), &pkt.num_segments, sizeof(uint32_t));
        memcpy(send_buf.data() + sizeof(uint32_t), pkt.segments.data(), pkt.num_segments * sizeof(Segment));
        memcpy(send_buf.data() + sizeof(uint32_t) + pkt.num_segments * sizeof(Segment), pkt.data.data(), pkt.data.size());

        // record start time on first send
        if (total_blasts_sent == 0) send_start_time = chrono::steady_clock::now();

        ssize_t s = sendto(sock, send_buf.data(), pkt_size, 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));
        if (s == -1) {
            perror("sendto");
            cerr << "[Sender] Failed to send blast (size=" << pkt_size << ")\n";
        }
        total_blasts_sent++;
        total_bytes_sent += pkt_size;

        cout << "[Sender] Blast " << (++blast_no) << " sent with records (" 
             << pkt.segments.front().start << "-" << pkt.segments.back().end << ")\n";

        // Wait for REC_MISS response
        char buf[1024];
        socklen_t addrlen = sizeof(receiver_addr);
        recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr *)&receiver_addr, &addrlen);
        string rec_miss(buf);
        cout << "[Sender] REC_MISS: " << rec_miss << "\n";
        // parse REC_MISS (array of ranges) to count missing records
        total_rec_miss_msgs++;
        // simple number extractor
        vector<long> nums;
        long cur = 0; bool in_num = false; bool neg = false;
        for (char ch : rec_miss) {
            if (ch == '-') { neg = true; in_num = true; cur = 0; }
            else if (isdigit((unsigned char)ch)) { in_num = true; cur = cur*10 + (ch - '0'); }
            else { if (in_num) { nums.push_back(neg ? -cur : cur); cur = 0; in_num = false; neg = false; } }
        }
        if (in_num) nums.push_back(neg ? -cur : cur);
        for (size_t i = 0; i + 1 < nums.size(); i += 2) {
            long a = nums[i]; long b = nums[i+1];
            if (b >= a) total_missing_records_reported += (uint64_t)(b - a + 1);
        }
    }

    // Send disconnect signal
    string disc = "DISCONNECT";
    // mark end time
    send_end_time = chrono::steady_clock::now();
    sendto(sock, disc.c_str(), disc.size(), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));
    cout << "[Sender] DISCONNECTED\n";

    // Print performance summary
    double secs = chrono::duration<double>(send_end_time - send_start_time).count();
    if (secs < 1e-6) secs = 1e-6;
    double throughput_bps = total_bytes_sent / secs; // bytes per second
    cout << "[Sender] Summary: blasts_sent=" << total_blasts_sent << ", bytes_sent=" << total_bytes_sent
         << ", rec_miss_msgs=" << total_rec_miss_msgs << ", missing_records_reported=" << total_missing_records_reported << "\n";
    cout << "[Sender] Duration=" << secs << "s, Throughput=" << (throughput_bps) << " B/s (" << (throughput_bps*8/1e6) << " Mbps)\n";
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cerr << "Usage: ./sender <file> <receiver_ip>\n";
        return 1;
    }

    string filename = argv[1];
    string ip = argv[2];

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    memset(&receiver_addr, 0, sizeof(receiver_addr));
    receiver_addr.sin_family = AF_INET;
    receiver_addr.sin_port = htons(9000);
    inet_pton(AF_INET, ip.c_str(), &receiver_addr.sin_addr);

    // Read file size
    ifstream fin(filename, ios::binary | ios::ate);
    negotiated_header.file_size = fin.tellg();
    fin.seekg(0);
    negotiated_header.record_size = 512; // fixed record size
    // Compute a safe records-per-blast (M) so the UDP datagram does not exceed typical limits
    const size_t MAX_UDP_PAYLOAD = 65507; // practical max UDP payload
    size_t rec_size = negotiated_header.record_size;
    size_t per_record_overhead = sizeof(Segment) + rec_size; // one Segment per record + data
    size_t max_records = 1;
    if (per_record_overhead > 0) {
        max_records = (MAX_UDP_PAYLOAD - sizeof(uint32_t)) / per_record_overhead;
        if (max_records == 0) max_records = 1;
    }
    // Desired M range requested: 200 - 10000. Use that when possible but fall back to max_records
    const size_t DESIRED_MIN_M = 200;
    const size_t DESIRED_MAX_M = 10000;
    // Choose M within desired range while respecting safe UDP payload (max_records).
    size_t chosen_M;
    if (max_records >= DESIRED_MIN_M) {
        chosen_M = min(max_records, DESIRED_MAX_M);
    } else {
        // can't satisfy desired minimum without exceeding UDP payload; use safe max_records
        chosen_M = max_records;
        cerr << "[Sender] Warning: safe UDP max_records=" << max_records << " is below desired minimum " << DESIRED_MIN_M << ". Using " << max_records << " instead.\n";
    }
    negotiated_header.M = (uint32_t)chosen_M;
    cout << "[Sender] Using records-per-blast M=" << negotiated_header.M << " (record_size=" << rec_size << ")\n";

    // Send file header
    sendto(sock, &negotiated_header, sizeof(negotiated_header), 0, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr));
    cout << "[Sender] Sent FILE_HDR\n";

    // Wait for FILE_HDR_ACK
    char ack[32];
    socklen_t addrlen = sizeof(receiver_addr);
    recvfrom(sock, ack, sizeof(ack), 0, (struct sockaddr *)&receiver_addr, &addrlen);
    cout << "[Sender] Received FILE_HDR_ACK\n";

    thread t_disk(disk_read_thread, filename);
    thread t_net(network_sender_thread);

    t_disk.join();
    t_net.join();

    close(sock);
    return 0;
}
