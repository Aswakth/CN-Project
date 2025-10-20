// garbler.cpp
// Usage: ./garbler <listen_port> <target_host> <target_port> <loss_percent>
// Example (sender -> garbler -> receiver):
// sender sends to garbler_port; garbler forwards to target (receiver) with random drops.
// Also listens replies from target and forwards back to original sender address.
// To use simply run: ./garbler 10000 127.0.0.1 9000 10

#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
using namespace std;
int main(int argc,char** argv){
    if(argc<5){ cerr<<"Usage: "<<argv[0]<<" <listen_port> <target_host> <target_port> <loss_percent>\n"; return 1;}
    int listen_port = atoi(argv[1]); string target_host=argv[2]; int target_port=atoi(argv[3]); int loss = atoi(argv[4]);
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in myaddr{}; myaddr.sin_family=AF_INET; myaddr.sin_addr.s_addr=INADDR_ANY; myaddr.sin_port=htons(listen_port);
    bind(sock, (sockaddr*)&myaddr, sizeof(myaddr));
    sockaddr_in target{}; target.sin_family=AF_INET; target.sin_port=htons(target_port); inet_pton(AF_INET, target_host.c_str(), &target.sin_addr);

    cout<<"Garbler listening on "<<listen_port<<" forwarding to "<<target_host<<":"<<target_port<<" with loss "<<loss<<"%\n";
    srand(time(NULL));
    // We'll remember last sender to forward replies
    sockaddr_in last_sender{}; socklen_t lastlen=sizeof(last_sender); bool have_last=false;
    while(true){
        char buf[65536]; sockaddr_in sa; socklen_t sl=sizeof(sa);
        int l = recvfrom(sock, buf, sizeof(buf), 0, (sockaddr*)&sa, &sl);
        if(l<=0) continue;
        // decide drop?
        int r = rand()%100;
        if(r < loss){
            // drop
            // optional: print small indicator
            cout<<'.'<<flush;
            continue;
        }
        // forward to target
        sendto(sock, buf, l, 0, (sockaddr*)&target, sizeof(target));
        // remember sender to forward back replies
        last_sender = sa; have_last = true;
        // now try to see reply from target (non-blocking peek)
        fd_set fds; FD_ZERO(&fds); FD_SET(sock,&fds);
        struct timeval tv; tv.tv_sec=0; tv.tv_usec=50000; // 50ms wait
        int rv = select(sock+1, &fds, NULL, NULL, &tv);
        if(rv>0){
            char rbuf[65536]; sockaddr_in sa2; socklen_t sl2=sizeof(sa2);
            int rl = recvfrom(sock, rbuf, sizeof(rbuf), 0, (sockaddr*)&sa2, &sl2);
            if(rl>0 && have_last){
                // decide to drop reply?
                int r2 = rand()%100;
                if(r2 < loss) { cout<<'R'<<flush; continue; }
                sendto(sock, rbuf, rl, 0, (sockaddr*)&last_sender, lastlen);
            }
        }
    }
    close(sock);
    return 0;
}
