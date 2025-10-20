// sender.cpp (enhanced version)
// Logs exact packet loss & retransmission stats and outputs CSV line for experiments

#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
using namespace std;

enum PTYPE { FILE_HDR=1, FILE_HDR_ACK=2, DATA_PKT=3, IS_BLAST_OVER=4, REC_MISS=5, DISCONNECT=6 };

uint64_t now_ms() {
    struct timeval tv; gettimeofday(&tv, nullptr);
    return tv.tv_sec*1000ULL + tv.tv_usec/1000ULL;
}

int main(int argc, char** argv) {
    if (argc < 6) {
        cerr << "Usage: " << argv[0] << " <receiver_host> <receiver_port> <file> <record_size> <M>\n";
        return 1;
    }
    string host = argv[1]; int port = atoi(argv[2]);
    string filename = argv[3];
    int rec_size = atoi(argv[4]); int M = atoi(argv[5]);

    ifstream ifs(filename, ios::binary | ios::ate);
    if (!ifs) { cerr << "Cannot open file.\n"; return 1; }
    size_t filesize = ifs.tellg(); ifs.seekg(0);
    vector<char> filebuf(filesize); ifs.read(filebuf.data(), filesize);
    size_t total_records = (filesize + rec_size - 1) / rec_size;

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in recvaddr{}; recvaddr.sin_family = AF_INET;
    recvaddr.sin_port = htons(port); inet_pton(AF_INET, host.c_str(), &recvaddr.sin_addr);

    auto sendpkt = [&](const vector<char>& pkt) {
        sendto(sock, pkt.data(), pkt.size(), 0, (sockaddr*)&recvaddr, sizeof(recvaddr));
    };

    // === Send header ===
    vector<char> hdr;
    hdr.push_back((char)FILE_HDR);
    uint32_t rs = htonl(rec_size); hdr.insert(hdr.end(), (char*)&rs, (char*)&rs + 4);
    uint64_t tr = htobe64((uint64_t)total_records); hdr.insert(hdr.end(), (char*)&tr, (char*)&tr + 8);
    uint16_t fnlen = htons(filename.size()); hdr.insert(hdr.end(), (char*)&fnlen, (char*)&fnlen + 2);
    hdr.insert(hdr.end(), filename.begin(), filename.end());
    uint32_t mnet = htonl(M); hdr.insert(hdr.end(), (char*)&mnet, (char*)&mnet + 4);
    sendpkt(hdr);

    // wait for ack
    fd_set fds; FD_ZERO(&fds); FD_SET(sock, &fds);
    struct timeval tv{2, 0};
    if (select(sock+1, &fds, NULL, NULL, &tv) <= 0) { cerr << "No ACK\n"; return 2; }
    char buf[2048]; sockaddr_in sa; socklen_t sl=sizeof(sa);
    int l = recvfrom(sock, buf, sizeof(buf), 0, (sockaddr*)&sa, &sl);
    if (buf[0] != FILE_HDR_ACK) { cerr << "Invalid ACK\n"; return 2; }

    cout << "HEADER OK, starting transfer..." << endl;
    cout.flush();

    uint64_t start = now_ms();
    uint64_t bytes_sent = 0;
    int total_retransmits = 0, total_loss_events = 0;

    size_t rec_idx = 1; int blast_id = 0;
    while (rec_idx <= total_records) {
        blast_id++;
        size_t startrec = rec_idx;
        size_t endrec = min(total_records, rec_idx + M - 1);

        // send packets for this blast
        for (size_t r = startrec; r <= endrec; r++) {
            vector<char> pkt;
            pkt.push_back((char)DATA_PKT);
            uint32_t bid = htonl(blast_id); pkt.insert(pkt.end(), (char*)&bid, (char*)&bid+4);
            uint64_t sr = htobe64(r); pkt.insert(pkt.end(), (char*)&sr, (char*)&sr+8);
            uint32_t one = htonl(1); pkt.insert(pkt.end(), (char*)&one, (char*)&one+4);
            size_t off = (r-1)*rec_size;
            size_t len = min((size_t)rec_size, filesize - off);
            pkt.insert(pkt.end(), filebuf.data()+off, filebuf.data()+off+len);
            sendpkt(pkt);
            bytes_sent += len;
        }

        // blast over signal
        vector<char> fin; fin.push_back((char)IS_BLAST_OVER);
        uint32_t bid = htonl(blast_id); fin.insert(fin.end(), (char*)&bid, (char*)&bid+4);
        uint64_t mst = htobe64(startrec); fin.insert(fin.end(), (char*)&mst, (char*)&mst+8);
        uint64_t mfin = htobe64(endrec); fin.insert(fin.end(), (char*)&mfin, (char*)&mfin+8);
        sendpkt(fin);

        // wait for REC_MISS
        while (1) {
            FD_ZERO(&fds); FD_SET(sock, &fds);
            tv.tv_sec = 2; tv.tv_usec = 0;
            int rv = select(sock+1, &fds, NULL, NULL, &tv);
            if (rv <= 0) { // timeout
                total_loss_events++;
                sendpkt(fin); // resend IS_BLAST_OVER
                continue;
            }
            int len = recvfrom(sock, buf, sizeof(buf), 0, (sockaddr*)&sa, &sl);
            if (buf[0] == REC_MISS) {
                int p=1; uint32_t bid2; memcpy(&bid2, buf+p,4); p+=4; bid2=ntohl(bid2);
                uint16_t nr; memcpy(&nr, buf+p,2); p+=2; nr=ntohs(nr);
                if (nr==0) break;
                total_retransmits += nr;
                for (int i=0;i<nr;i++){
                    uint64_t s,e; memcpy(&s,buf+p,8); p+=8; memcpy(&e,buf+p,8); p+=8;
                    s=be64toh(s); e=be64toh(e);
                    for(uint64_t rr=s; rr<=e; rr++){
                        vector<char> pkt;
                        pkt.push_back((char)DATA_PKT);
                        uint32_t bid3 = htonl(blast_id); pkt.insert(pkt.end(), (char*)&bid3, (char*)&bid3+4);
                        uint64_t sr2 = htobe64(rr); pkt.insert(pkt.end(), (char*)&sr2, (char*)&sr2+8);
                        uint32_t one2 = htonl(1); pkt.insert(pkt.end(), (char*)&one2, (char*)&one2+4);
                        size_t off=(rr-1)*rec_size; size_t len=min((size_t)rec_size, filesize-off);
                        pkt.insert(pkt.end(), filebuf.data()+off, filebuf.data()+off+len);
                        sendpkt(pkt); bytes_sent += len;
                    }
                }
                sendpkt(fin);
            }
        }
        rec_idx = endrec + 1;
    }

    // ------------------ Summary & CSV ------------------

    // Compute throughput
    uint64_t end = now_ms();
    double secs = (end - start) / 1000.0;
    double throughput = (bytes_sent / secs) / (1024.0*1024.0) * 8.0; // Mbps

    // Print human-readable summary (optional, can keep)
    cerr << "---- SUMMARY ----\n";
    cerr << "File: " << filename << "\n";
    cerr << "Bytes: " << bytes_sent << "  Time: " << secs << " s\n";
    cerr << "Throughput: " << throughput << " Mbps\n";
    cerr << "Loss Events: " << total_loss_events << "  Retransmits: " << total_retransmits << "\n";

    // === GUARANTEE last line is CSV for Python script ===
    cout << filename << "," << throughput << "," << total_loss_events << "," << total_retransmits << endl;
    cout.flush();
    // Disconnect
    char dis = (char)DISCONNECT; sendpkt({dis});
    close(sock);
}
