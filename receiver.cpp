// receiver_multi.cpp
#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
using namespace std;

enum PTYPE { FILE_HDR=1, FILE_HDR_ACK=2, DATA_PKT=3, IS_BLAST_OVER=4, REC_MISS=5, DISCONNECT=6 };

int main(int argc, char** argv) {
    if(argc < 3){ cerr << "Usage: " << argv[0] << " <listen_port> <out_dir>\n"; return 1; }
    int port = atoi(argv[1]);
    string outdir = argv[2];

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in myaddr{};
    myaddr.sin_family = AF_INET;
    myaddr.sin_port = htons(port);
    myaddr.sin_addr.s_addr = INADDR_ANY;
    bind(sock, (sockaddr*)&myaddr, sizeof(myaddr));

    cout << "Receiver listening on port " << port << "\n";

    vector<char> filebuf;
    vector<char> recv_marker;
    uint32_t record_size = 0;
    uint64_t total_records = 0;
    string filename;
    bool header_done = false;

    while(true) {
        char buf[65536];
        sockaddr_in sa;
        socklen_t sl = sizeof(sa);
        int l = recvfrom(sock, buf, sizeof(buf), 0, (sockaddr*)&sa, &sl);
        if(l <= 0) continue;

        uint8_t t = buf[0];

        if(t == FILE_HDR) {
            // Reset state for new file
            filebuf.clear();
            recv_marker.clear();
            header_done = false;

            int p=1;
            uint32_t rs; memcpy(&rs, buf+p,4); p+=4; record_size = ntohl(rs);
            uint64_t tr; memcpy(&tr, buf+p,8); p+=8; total_records = be64toh(tr);
            uint16_t fnlen; memcpy(&fnlen, buf+p,2); p+=2; fnlen = ntohs(fnlen);
            filename = string(buf+p, buf+p + fnlen); p += fnlen;

            cout << "Received FILE_HDR: file=" << filename
                 << " rec_size=" << record_size
                 << " total_records=" << total_records << "\n";

            filebuf.assign(total_records * record_size, 0);
            recv_marker.assign(total_records + 1, 0); // 1-based indexing
            header_done = true;

            char ack = (char)FILE_HDR_ACK;
            sendto(sock, &ack, 1, 0, (sockaddr*)&sa, sl);

        } else if(t == DATA_PKT && header_done) {
            int p=1;
            uint32_t bid; memcpy(&bid, buf+p,4); p+=4; bid = ntohl(bid);
            uint64_t startrec; memcpy(&startrec, buf+p,8); p+=8; startrec = be64toh(startrec);
            uint32_t numrec; memcpy(&numrec, buf+p,4); p+=4; numrec = ntohl(numrec);
            size_t got = l - p;

            for(size_t i=0; i<numrec; i++){
                uint64_t rid = startrec + i;
                if(rid > total_records) break;
                size_t off = (rid-1) * record_size;
                size_t copy_len = min((size_t)record_size, ((rid==total_records) ? (filebuf.size() - off) : (size_t)record_size));
                size_t payload_off = i * record_size;
                if(payload_off >= got) break;
                size_t available = min(copy_len, got - payload_off);
                memcpy(&filebuf[off], &buf[p + payload_off], available);
                recv_marker[rid] = 1;
            }

        } else if(t == IS_BLAST_OVER && header_done) {
            int p=1;
            uint32_t bid; memcpy(&bid, buf+p,4); p+=4; bid = ntohl(bid);
            uint64_t mst; memcpy(&mst, buf+p,8); p+=8; mst = be64toh(mst);
            uint64_t mfin; memcpy(&mfin, buf+p,8); p+=8; mfin = be64toh(mfin);

            vector<pair<uint64_t,uint64_t>> miss;
            uint64_t cur = mst;
            while(cur <= mfin) {
                if(recv_marker[cur]) { cur++; continue; }
                uint64_t s = cur;
                while(cur <= mfin && !recv_marker[cur]) cur++;
                uint64_t e = cur-1;
                miss.push_back({s,e});
            }

            vector<char> pkt; pkt.push_back((char)REC_MISS);
            uint32_t bidn = htonl(bid); pkt.insert(pkt.end(), (char*)&bidn, (char*)&bidn+4);
            uint16_t nr = htons((uint16_t)miss.size()); pkt.insert(pkt.end(), (char*)&nr, (char*)&nr+2);
            for(auto &r: miss){
                uint64_t s = htobe64(r.first), e = htobe64(r.second);
                pkt.insert(pkt.end(), (char*)&s, (char*)&s+8);
                pkt.insert(pkt.end(), (char*)&e, (char*)&e+8);
            }
            sendto(sock, pkt.data(), pkt.size(), 0, (sockaddr*)&sa, sl);

            if(mfin == total_records && miss.empty()) {
                string outpath = outdir + "/" + filename;
                ofstream ofs(outpath, ios::binary);
                ofs.write(filebuf.data(), filebuf.size());
                ofs.close();
                cout << "Wrote file to " << outpath << "\n";
            }

        } else if(t == DISCONNECT) {
            cout << "Received DISCONNECT, closing current transfer\n";
            // After DISCONNECT, receiver resets and waits for next sender
            filebuf.clear();
            recv_marker.clear();
            header_done = false;
        }
    }

    close(sock);
    return 0;
}
